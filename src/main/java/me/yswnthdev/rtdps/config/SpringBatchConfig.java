package me.yswnthdev.rtdps.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import me.yswnthdev.rtdps.entity.Customer;
import me.yswnthdev.rtdps.listener.JobCompletionListener;
import me.yswnthdev.rtdps.service.ConflictAwareCustomerWriter;
import me.yswnthdev.rtdps.service.ConflictPolicy;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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
    private final DataSource dataSource;

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

    @Bean
    public NamedParameterJdbcTemplate namedJdbcTemplate() {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    /**
     * Step-scoped so the {@code conflictPolicy} JobParameter resolves per execution.
     * The writer is also a {@code StepExecutionListener} — Spring Batch auto-registers it.
     */
    @Bean
    @StepScope
    public ConflictAwareCustomerWriter customerWriter(
            NamedParameterJdbcTemplate namedJdbcTemplate,
            @Value("#{jobParameters['conflictPolicy']}") String policyName) {
        ConflictPolicy policy = ConflictPolicy.parseOrDefault(policyName);
        log.info("[SpringBatchConfig] Writer configured with conflict policy: {}", policy);
        return new ConflictAwareCustomerWriter(namedJdbcTemplate, policy);
    }

    @Bean
    public Step importStep(SynchronizedItemStreamReader<Customer> reader,
                           ConflictAwareCustomerWriter customerWriter) {
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
     * Dedicated pool used by the {@link #jobLauncher(JobRepository, TaskExecutor)}
     * to actually run jobs. Kept distinct from {@link #taskExecutor()} (which
     * threads the chunked import step) so a flood of submissions cannot starve
     * worker threads inside an already-running step, and vice versa.
     */
    @Bean(name = "jobLauncherExecutor")
    public ThreadPoolTaskExecutor jobLauncherExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(50);              // ~50 dispatched-but-not-yet-started jobs
        executor.setThreadNamePrefix("job-launcher-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(300);   // let in-flight imports finish on SIGTERM
        executor.initialize();
        return executor;
    }

    /**
     * Async {@code JobLauncher} backed by {@link #jobLauncherExecutor()}.
     * {@code @Primary} so it wins over Spring Boot's auto-configured synchronous
     * launcher without triggering a {@code BeanDefinitionOverrideException}.
     * <p>
     * Why: the Kafka consumer thread used to block inside {@code launcher.run()}
     * for the entire duration of the import. A multi-minute CSV would silently
     * exceed {@code max.poll.interval.ms} → consumer eviction → redelivery →
     * duplicate {@code JobInstance}. With this bean in place, {@code run()}
     * returns immediately after dispatching the job; the Kafka offset commits
     * on the next poll and the consumer is free to pick up the next request.
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
