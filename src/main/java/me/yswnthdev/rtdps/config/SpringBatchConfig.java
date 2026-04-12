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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
    public JdbcBatchItemWriter<Customer> writer() {
        return new JdbcBatchItemWriterBuilder<Customer>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO customers (id, first_name, last_name, email, gender, contact_no, country, dob)
                        VALUES (:id, :firstName, :lastName, :email, :gender, :contactNo, :country, :dob)
                        ON CONFLICT (id) DO NOTHING
                        """)
                .beanMapped()
                .assertUpdates(false)
                .build();
    }

    @Bean
    public Step importStep(SynchronizedItemStreamReader<Customer> reader, JdbcBatchItemWriter<Customer> writer) {
        return new StepBuilder("csv-step", jobRepository)
                .<Customer, Customer>chunk(20, transactionManager)
                .reader(reader)
                .processor(processor())
                .writer(writer)
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
}
