package com.example.clientkafkabatch.core.config.batch;

import com.example.clientkafkabatch.domain.payload.Client;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Value("${inputFile}")
    private String url;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final ClientItemWriter clientItemWriter;
    private final ClientItemProcessor clientItemProcessor;

    public BatchConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
                       ClientItemWriter clientItemWriter, ClientItemProcessor clientItemProcessor) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.clientItemWriter = clientItemWriter;
        this.clientItemProcessor = clientItemProcessor;
    }

    @Bean
    public Job importClient() {
        return jobBuilderFactory.get("importClientInfo")
                .incrementer(new RunIdIncrementer())
                .start(fromFileIntoDatabase())
                .build();
    }

    @Bean
    public Step fromFileIntoDatabase() {
        return stepBuilderFactory.get("FromFileToDatabases")
                .<Client, Client>chunk(10)
                .reader(clientInfoItemReader())
                .processor(clientItemProcessor)
                .writer(clientItemWriter)
                .taskExecutor(taskExecute())
                .build();
    }

    @Bean
    public FlatFileItemReader<Client> clientInfoItemReader() {
        return new FlatFileItemReaderBuilder<Client>()
                .resource(new ClassPathResource(url))
                .name("ClientInfoReader")
                .delimited()
                .delimiter(",")
                .names(new String[]{"id", "name", "address", "email"})
                .linesToSkip(1)
                .targetType(Client.class)
                .build();
    }

    @Bean
    public TaskExecutor taskExecute() {
        var executor = new ThreadPoolTaskExecutor();
        System.out.println("SIZE : " + executor.getQueueSize());
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(10);
        executor.setThreadNamePrefix("Thread N-> :  ");
        return executor;
    }
}
