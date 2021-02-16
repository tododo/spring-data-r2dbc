package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.relational.core.query.CriteriaDefinition.*;

import io.r2dbc.spi.Row;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.query.R2dbcExampleMapper;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.Parameter;

public class R2dbcExampleMapperTests {

	RelationalMappingContext mappingContext;
	R2dbcExampleMapper exampleMapper;

	@BeforeEach
	public void before() {

		R2dbcCustomConversions conversions = new R2dbcCustomConversions(
				Arrays.asList(StringToMapConverter.INSTANCE, MapToStringConverter.INSTANCE,
						CustomConversionPersonToOutboundRowConverter.INSTANCE, RowToCustomConversionPerson.INSTANCE));

		mappingContext = new R2dbcMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());

		exampleMapper = new R2dbcExampleMapper(mappingContext);
	}

	@Test
	void queryByExampleWithId() {

		Example<Person> example = Example.of(new Person("id1", "Frodo", "Baggins", Instant.now(), LocalDateTime.now()));

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()).hasValueSatisfying(criteria -> {
			assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("id"));
			assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
			assertThat(criteria.getValue()).isEqualTo("id1");
		});
	}

	@Test
	void queryByExampleWithFirstname() {

		Person person = new Person();
		person.setFirstname("Frodo");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo')");
	}

	@Test
	void queryByExampleWithFirstnameAndLastname() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo') AND (lastname = 'Baggins')");
	}

	@Test
	void queryByExampleWithNullMatchingLastName() {

		Person person = new Person();
		person.setLastname("Baggins");

		ExampleMatcher matcher = ExampleMatcher.matching().withIncludeNullValues();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(lastname IS NULL OR lastname = 'Baggins')");
	}

	@Test
	void queryByExampleWithNullMatchingFirstnameAndLastname() {

		Person person = new Person();
		person.setFirstname("Bilbo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = ExampleMatcher.matching().withIncludeNullValues();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname IS NULL OR firstname = 'Bilbo') AND (lastname IS NULL OR lastname = 'Baggins')");
	}

	@Test
	void queryByExampleWithFirstnameAndLastnameIgnoringFirstname() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = ExampleMatcher.matching().withIgnorePaths("firstname");
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(lastname = 'Baggins')");
	}

	@Test
	void queryByExampleWithFirstnameAndLastnameWithNullMatchingIgnoringFirstName() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = ExampleMatcher.matching().withIncludeNullValues().withIgnorePaths("firstname");
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(lastname IS NULL OR lastname = 'Baggins')");
	}

	@Test
	void queryByExampleWithFirstnameWithStringMatchingOnTheEnding() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = ExampleMatcher.matching().withStringMatcher(ExampleMatcher.StringMatcher.ENDING);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE '%do')");
	}

	@Test
	void queryByExampleWithFirstnameWithStringMatchingAtTheBeginning() {

		Person person = new Person();
		person.setFirstname("Fro");

		ExampleMatcher matcher = ExampleMatcher.matching().withStringMatcher(ExampleMatcher.StringMatcher.STARTING);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE 'Fro%')");
	}

	@Test
	void queryByExampleWithFirstnameWithStringMatchingContaining() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = ExampleMatcher.matching().withStringMatcher(ExampleMatcher.StringMatcher.CONTAINING);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE '%do%')");
	}

	@Test
	void queryByExampleWithFirstnameWithStringMatchingRegEx() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = ExampleMatcher.matching().withStringMatcher(ExampleMatcher.StringMatcher.REGEX);
		Example<Person> example = Example.of(person, matcher);

		assertThatIllegalStateException().isThrownBy(() -> exampleMapper.getMappedExample(example))
				.withMessageContaining("REGEX is not supported!");
	}

	@Test
	void queryByExampleWithFirstnameWithFieldSpecificStringMatcherEndsWith() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = ExampleMatcher.matching().withMatcher("firstname",
				ExampleMatcher.GenericPropertyMatcher::endsWith);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE '%do')");
	}

	@Test
	void queryByExampleWithFirstnameWithFieldSpecificStringMatcherStartsWith() {

		Person person = new Person();
		person.setFirstname("Fro");

		ExampleMatcher matcher = ExampleMatcher.matching().withMatcher("firstname",
				ExampleMatcher.GenericPropertyMatcher::startsWith);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE 'Fro%')");
	}

	@Test
	void queryByExampleWithFirstnameWithFieldSpecificStringMatcherContains() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = ExampleMatcher.matching().withMatcher("firstname",
				ExampleMatcher.GenericPropertyMatcher::contains);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE '%do%')");
	}

	@Test
	void queryByExampleWithFirstnameIgnoreCase() {

		Person person = new Person();
		person.setFirstname("Frodo");

		ExampleMatcher matcher = ExampleMatcher.matching().withIgnoreCase(true);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo')");
	}

	@AllArgsConstructor
	@NoArgsConstructor
	@Getter
	@Setter
	static class Person {
		@Id String id;
		String firstname, lastname;
		Instant instant;
		LocalDateTime localDateTime;
	}

	@Getter
	@Setter
	@RequiredArgsConstructor
	static class ConstructorAndPropertyPopulation {
		final String firstname;
		String lastname;
	}

	@AllArgsConstructor
	static class WithEnum {
		@Id String id;
		Condition condition;
	}

	enum Condition {
		Mint, Used
	}

	@AllArgsConstructor
	static class PersonWithConversions {
		@Id String id;
		Map<String, String> nested;
		NonMappableEntity unsupported;
	}

	@RequiredArgsConstructor
	static class WithPrimitiveId {

		@Id final long id;
	}

	static class CustomConversionPerson {

		String foo;
		NonMappableEntity entity;
	}

	static class NonMappableEntity {}

	@ReadingConverter
	enum StringToMapConverter implements Converter<String, Map<String, String>> {

		INSTANCE;

		@Override
		public Map<String, String> convert(String source) {

			if (source != null) {
				return Collections.singletonMap(source, source);
			}

			return null;
		}
	}

	@WritingConverter
	enum MapToStringConverter implements Converter<Map<String, String>, String> {

		INSTANCE;

		@Override
		public String convert(Map<String, String> source) {

			if (!source.isEmpty()) {
				return source.keySet().iterator().next();
			}

			return null;
		}
	}

	@WritingConverter
	enum CustomConversionPersonToOutboundRowConverter implements Converter<CustomConversionPerson, OutboundRow> {

		INSTANCE;

		@Override
		public OutboundRow convert(CustomConversionPerson source) {

			OutboundRow row = new OutboundRow();
			row.put("foo_column", Parameter.from(source.foo));
			row.put("entity", Parameter.from("nested_entity"));

			return row;
		}
	}

	@ReadingConverter
	enum RowToCustomConversionPerson implements Converter<Row, CustomConversionPerson> {

		INSTANCE;

		@Override
		public CustomConversionPerson convert(Row source) {

			CustomConversionPerson person = new CustomConversionPerson();
			person.foo = source.get("foo_column", String.class);

			Object nested_entity = source.get("nested_entity");
			person.entity = nested_entity != null ? new NonMappableEntity() : null;

			return person;
		}
	}
}
