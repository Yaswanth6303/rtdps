package me.yswnthdev.rtdps.config;

import lombok.RequiredArgsConstructor;
import me.yswnthdev.rtdps.entity.Customer;
import me.yswnthdev.rtdps.listener.JobCompletionListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class BatchExportConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;
    private final JobCompletionListener jobCompletionListener;

    @Bean
    @StepScope
    public JdbcCursorItemReader<Customer> exportReader() {
        return new JdbcCursorItemReaderBuilder<Customer>()
                .dataSource(dataSource)
                .name("customerJdbcReader")
                .sql("SELECT id, first_name, last_name, email, gender, contact_no, country, dob FROM customers ORDER BY id ASC")
                .rowMapper(new BeanPropertyRowMapper<>(Customer.class))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<Customer> exportWriter(
            @Value("#{jobParameters['outputPath']}") String outputPath) {

        BeanWrapperFieldExtractor<Customer> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{
                "id", "firstName", "lastName", "email",
                "gender", "contactNo", "country", "dob"
        });

        DelimitedLineAggregator<Customer> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(extractor);

        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource(outputPath));
        writer.setHeaderCallback(w -> w.write("id,firstName,lastName,email,gender,contactNo,country,dob"));
        writer.setLineAggregator(aggregator);

        return writer;
    }

    @Bean
    public Step exportStep(JdbcCursorItemReader<Customer> exportReader, FlatFileItemWriter<Customer> exportWriter) {
        return new StepBuilder("export-step", jobRepository)
                .<Customer, Customer>chunk(100, transactionManager)
                .reader(exportReader)
                .writer(exportWriter)
                .build();
    }

    @Bean
    public Job exportJob(Step exportStep) {
        return new JobBuilder("exportCustomers", jobRepository)
                .listener(jobCompletionListener)
                .start(exportStep)
                .build();
    }
}
