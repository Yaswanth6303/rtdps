package me.yswnthdev.rtdps.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import me.yswnthdev.rtdps.entity.Customer;
import me.yswnthdev.rtdps.listener.JobCompletionListener;
import me.yswnthdev.rtdps.service.CustomerProcessor;
import me.yswnthdev.rtdps.utils.CustomerFieldSetMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class SpringBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final JobCompletionListener jobCompletionListener;

    @Bean
    @StepScope
    public SynchronizedItemStreamReader<Customer> reader(
            @Value("#{jobParameters['filePath']}") String filePath,
            @Value("#{jobParameters['trackerId']}") String trackerId) {
        log.info("[SpringBatchConfig] Reading CSV from: {}", filePath);
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(filePath));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper(trackerId));
        // Wrapped in SynchronizedItemStreamReader so multiple worker threads can read safely.
        return new SynchronizedItemStreamReaderBuilder<Customer>()
                .delegate(itemReader)
                .build();
    }

    private LineMapper<Customer> lineMapper(String trackerId) {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setStrict(false);
        tokenizer.setNames(
                "id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new CustomerFieldSetMapper(trackerId));
        return lineMapper;
    }

    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }

    /**
     * Idempotent upsert writer — {@code ON CONFLICT (id) DO NOTHING} silently
     * skips rows whose id already exists, so redelivered Kafka messages don't
     * produce duplicate customers.
     */
    @Bean
    public JdbcBatchItemWriter<Customer> customerWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Customer>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO customers (id, first_name, last_name, email, gender, contact_no, country, dob)
                        VALUES (:id, :firstName, :lastName, :email, :gender, :contactNo, :country, :dob)
                        ON CONFLICT (id) DO NOTHING
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    public Step importStep(SynchronizedItemStreamReader<Customer> reader,
                           JdbcBatchItemWriter<Customer> customerWriter) {
        return new StepBuilder("csv-step", jobRepository)
                .<Customer, Customer>chunk(20, transactionManager)
                .reader(reader)
                .processor(processor())
                .writer(customerWriter)
                .faultTolerant()
                .skipLimit(100)
                .skip(FlatFileParseException.class)
                .skip(NumberFormatException.class)
                .retryLimit(3)
                .retry(DataAccessException.class)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Job importJob(Step importStep) {
        return new JobBuilder("importCustomers", jobRepository)
                .listener(jobCompletionListener)
                .flow(importStep)
                .end()
                .build();
    }

    /** Threads the chunked import step so one CSV is processed in parallel. */
    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(20);
        executor.setThreadNamePrefix("batch-thread-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }

    /**
     * Dedicated pool used by {@link #asyncJobLauncher} to actually run jobs —
     * kept distinct from {@link #taskExecutor()} so submissions cannot starve
     * worker threads inside an already-running step (and vice versa).
     */
    @Bean(name = "jobLauncherExecutor")
    public ThreadPoolTaskExecutor jobLauncherExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("job-launcher-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(300);
        executor.initialize();
        return executor;
    }

    /**
     * Async {@code JobLauncher} — {@code run()} returns immediately after
     * dispatching the job so the Kafka consumer thread isn't blocked for the
     * duration of a multi-minute import. Prevents {@code max.poll.interval.ms}
     * eviction and lets the consumer commit its offset and pick up the next
     * message right away. {@code @Primary} so it wins over Spring Boot's
     * auto-configured synchronous launcher.
     */
    @Bean
    @Primary
    public JobLauncher asyncJobLauncher(JobRepository jobRepository,
                                         @Qualifier("jobLauncherExecutor") TaskExecutor jobLauncherExecutor) throws Exception {
        TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(jobLauncherExecutor);
        launcher.afterPropertiesSet();
        return launcher;
    }
}
