/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.repository.support;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.stereotype.Service;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Abstract integration tests for {@link SimpleR2dbcRepository} to be ran against various databases.
 *
 * @author Mark Paluch
 */
@ContextConfiguration
public abstract class AbstractSimpleR2dbcRepositoryIntegrationTests extends R2dbcIntegrationTestSupport {
	@Autowired
	GenericApplicationContext context;
	@Autowired private DatabaseClient databaseClient;

	@Autowired private RelationalMappingContext mappingContext;

	@Autowired private ReactiveDataAccessStrategy strategy;

	@Autowired LegoSetRepository repository;

	@Autowired  private TransactionalService service;

	JdbcTemplate jdbc;



	@Configuration
	@EnableTransactionManagement
	@EnableR2dbcRepositories
	@ComponentScan
	static class Config extends AbstractR2dbcConfiguration {

		@Autowired
		GenericApplicationContext context;

		@Override
		@Bean
		public ConnectionFactory connectionFactory() {
			return lookup();
		}

		ConnectionFactory lookup() {
			return context.getBean("theConnectionFactory", ConnectionFactory.class);
		}

		@Bean
		ReactiveTransactionManager txMgr(ConnectionFactory connectionFactory) {
			return new R2dbcTransactionManager(connectionFactory);
		}


	}
	@Before
	public void before() {


		this.jdbc = createJdbcTemplate(createDataSource());
		try {
			this.jdbc.execute("DROP TABLE legoset");
		} catch (DataAccessException e) {}

		this.jdbc.execute(getCreateTableStatement());
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	protected abstract DataSource createDataSource();

	/**
	 * Returns the the CREATE TABLE statement for table {@code legoset} with the following three columns:
	 * <ul>
	 * <li>id integer (primary key), not null, auto-increment</li>
	 * <li>name varchar(255), nullable</li>
	 * <li>manual integer, nullable</li>
	 * </ul>
	 *
	 * @return the CREATE TABLE statement for table {@code legoset} with three columns.
	 */
	protected abstract String getCreateTableStatement();

	@Test
	public void shouldRollbackRepositoryTransactions() {
		service.shouldRollbackRepository().as(StepVerifier::create).expectError().verify();
		Integer count = jdbc.queryForObject("SELECT count(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	public void shouldRollbackDatabaseClientInsertTransactions() {
		service.shouldRollbackDatabaseClientInsertSpec().as(StepVerifier::create).expectError().verify();
		Integer count = jdbc.queryForObject("SELECT count(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	public void shouldRollbackDatabaseClientExecuteTransactions() {
		service.shouldRollbackDatabaseClient().as(StepVerifier::create).expectError().verify();
		Integer count = jdbc.queryForObject("SELECT count(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	public void shouldSaveNewObject() {

		LegoSet legoSet = new LegoSet(null, "SCHAUFELRADBAGGER", 12);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isNotNull();
				}).verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * FROM legoset");
		assertThat(map).containsEntry("name", "SCHAUFELRADBAGGER").containsEntry("manual", 12).containsKey("id");
	}

	@Test
	public void shouldUpdateObject() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		LegoSet legoSet = new LegoSet(id, "SCHAUFELRADBAGGER", 12);
		legoSet.setManual(14);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * FROM legoset");
		assertThat(map).containsEntry("name", "SCHAUFELRADBAGGER").containsEntry("manual", 14).containsKey("id");
	}

	@Test
	public void shouldSaveObjectsUsingIterable() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);
		LegoSet legoSet3 = new LegoSet(null, "RALLYEAUTO", 14);
		LegoSet legoSet4 = new LegoSet(null, "VOLTRON", 15);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2, legoSet3, legoSet4)) //
				.map(LegoSet::getManual) //
				.as(StepVerifier::create) //
				.expectNext(12) //
				.expectNext(13) //
				.expectNext(14) //
				.expectNext(15) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(4);
	}

	@Test
	public void shouldSaveObjectsUsingPublisher() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		repository.saveAll(Flux.just(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(2);
	}

	@Test
	public void shouldFindById() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		repository.findById(id) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual.getId()).isEqualTo(id);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test
	public void shouldExistsById() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		repository.existsById(id) //
				.as(StepVerifier::create) //
				.expectNext(true)//
				.verifyComplete();

		repository.existsById(42) //
				.as(StepVerifier::create) //
				.expectNext(false)//
				.verifyComplete();
	}

	@Test
	public void shouldExistsByIdPublisher() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		repository.existsById(Mono.just(id)) //
				.as(StepVerifier::create) //
				.expectNext(true)//
				.verifyComplete();

		repository.existsById(Mono.just(42)) //
				.as(StepVerifier::create) //
				.expectNext(false)//
				.verifyComplete();
	}

	@Test
	public void shouldFindByAll() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");

		repository.findAll() //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).hasSize(2).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	public void shouldFindAllByIdUsingIterable() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");

		List<Integer> ids = jdbc.queryForList("SELECT id FROM legoset", Integer.class);

		repository.findAllById(ids) //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).hasSize(2).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	public void shouldFindAllByIdUsingPublisher() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");

		List<Integer> ids = jdbc.queryForList("SELECT id FROM legoset", Integer.class);

		repository.findAllById(Flux.fromIterable(ids)) //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).hasSize(2).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	public void shouldCount() {

		repository.count() //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");

		repository.count() //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test
	public void shouldDeleteById() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		repository.deleteById(id) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	public void shouldDeleteByIdPublisher() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		repository.deleteById(Mono.just(id)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	public void shouldDelete() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		LegoSet legoSet = new LegoSet(id, "SCHAUFELRADBAGGER", 12);

		repository.delete(legoSet) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	public void shouldDeleteAllUsingIterable() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		LegoSet legoSet = new LegoSet(id, "SCHAUFELRADBAGGER", 12);

		repository.deleteAll(Collections.singletonList(legoSet)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	public void shouldDeleteAllUsingPublisher() {

		jdbc.execute("INSERT INTO legoset (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id FROM legoset", Integer.class);

		LegoSet legoSet = new LegoSet(id, "SCHAUFELRADBAGGER", 12);

		repository.deleteAll(Mono.just(legoSet)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Data
	@Table("legoset")
	@AllArgsConstructor
	@NoArgsConstructor
	static class LegoSet implements Persistable<Integer> {
		@Id Integer id;
		String name;
		Integer manual;
		@Override
		public boolean isNew() {
			return id == null;
		}
	}

	static class AlwaysNewLegoSet extends LegoSet {

		AlwaysNewLegoSet(Integer id, String name, Integer manual) {
			super(id, name, manual);
		}

		@Override
		public boolean isNew() {
			return true;
		}
	}

	@Service
	static class TransactionalService {

		private LegoSetRepository repository;
		private DatabaseClient databaseClient;

		public TransactionalService(DatabaseClient databaseClient, LegoSetRepository repository) {
			this.repository = repository;
			this.databaseClient = databaseClient;
		}

		@Transactional
		public Mono<Void> shouldRollbackRepository() {
			LegoSet l1 = new LegoSet(null, "good", 12);

			Mono<LegoSet> m1 = repository.save(l1);
			LegoSet l2 = new LegoSet(null,
															 "ggoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodood", 12);
			Mono<LegoSet> m2 = repository.save(l2);
			return m1.then(m2).then();
		}
		private String badName =
			"ggoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodgoodood";

		@Transactional
		public Mono<Void> shouldRollbackDatabaseClient() {
			Mono<Void> m1 = databaseClient.execute("INSERT INTO LEGOSET  (name, manual) VALUES(@name, @manual)").bind(0,
																																																					 badName).bind(1,
																																																								 1).then();
			Mono<Void> m2 = databaseClient.execute("INSERT INTO LEGOSET  (name, manual) VALUES(@name, @manual)").bind(0,
																																																								"good").bind(1,
																																																															1).then();
			return m1.then(m2).then();
		}

		@Transactional
		public Mono<Void> shouldRollbackDatabaseClientInsertSpec() {
			Mono<Void> m1 = databaseClient.insert().into("legoset").value("name", badName).value("manual",
																																													 Integer.valueOf(1)).then();
			Mono<Void> m2 = databaseClient.insert().into("legoset").value("name", "good").value("manual",
																																													Integer.valueOf(1)).then();
			return m1.then(m2).then();
		}

		@Transactional
		public Flux<Object> emitTransactionIds(Mono<Void> prepareTransaction, String idStatement) {

			Flux<Object> txId = databaseClient.execute(idStatement) //
				.map((row, md) -> row.get(0)) //
				.all();

			return prepareTransaction.thenMany(txId.concatWith(txId));
		}

		@Transactional
		public Flux<Integer> shouldRollbackTransactionUsingTransactionalOperator(String insertStatement) {

			return databaseClient.execute(insertStatement) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated() //
				.thenMany(Mono.fromSupplier(() -> {
					throw new IllegalStateException("failed");
				}));
		}

		public DatabaseClient getDatabaseClient() {
			return databaseClient;
		}
	}
}
