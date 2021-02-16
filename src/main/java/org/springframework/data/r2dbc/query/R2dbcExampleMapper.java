package org.springframework.data.r2dbc.query;

import static org.springframework.data.domain.ExampleMatcher.*;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.data.domain.Example;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.util.Assert;

/**
 * Class to transform an {@link org.springframework.data.domain.Example} into a
 * {@link org.springframework.data.relational.core.query.Query}.
 */
public class R2dbcExampleMapper {

	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;

	public R2dbcExampleMapper(MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext) {

		this.mappingContext = mappingContext;
	}

	public <T> Query getMappedExample(Example<T> example) {
		return getMappedExample(example, mappingContext.getRequiredPersistentEntity(example.getProbeType()));
	}

	private <T> Query getMappedExample(Example<T> example, RelationalPersistentEntity<?> entity) {

		Assert.notNull(example, "Example must not be null!");
		Assert.notNull(entity, "MongoPersistentEntity must not be null!");

		Criteria criteria = Criteria.empty();

		BeanWrapper beanWrapper = new BeanWrapperImpl(example.getProbe());

		PropertyDescriptor[] propertyDescriptors = beanWrapper.getPropertyDescriptors();
		for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {

			if (propertyDescriptor.getName().equals("class")) {
				continue;
			}

			if (example.getMatcher().getIgnoredPaths().contains(propertyDescriptor.getName())) {
				continue;
			}

			try {
				propertyDescriptor.getReadMethod().setAccessible(true);
				Object property = propertyDescriptor.getReadMethod().invoke(example.getProbe());
				if (property != null) {

					String columnName = entity.getPersistentProperty(propertyDescriptor.getName()).getColumnName().getReference();

					Criteria propertyCriteria;

					// First, check the overall matcher for settings
					StringMatcher stringMatcher = example.getMatcher().getDefaultStringMatcher();
					boolean ignoreCase = example.getMatcher().isIgnoreCaseEnabled();

					// Then, apply any property-specific overrides
					if (example.getMatcher().getPropertySpecifiers().hasSpecifierForPath(propertyDescriptor.getName())) {

						PropertySpecifier specifier = example.getMatcher().getPropertySpecifiers()
								.getForPath(propertyDescriptor.getName());

						if (specifier.getStringMatcher() != null) {
							stringMatcher = specifier.getStringMatcher();
						}

						if (specifier.getIgnoreCase() != null) {
							ignoreCase = specifier.getIgnoreCase();
						}
					}

					// Assemble the property's criteria
					switch (stringMatcher) {
						case DEFAULT:
						case EXACT:
							propertyCriteria = example.getMatcher().getNullHandler() == NullHandler.INCLUDE
									? Criteria.where(columnName).isNull().or(columnName).is(property).ignoreCase(ignoreCase)
									: Criteria.where(columnName).is(property).ignoreCase(ignoreCase);
							break;
						case ENDING:
							propertyCriteria = example.getMatcher().getNullHandler() == NullHandler.INCLUDE
									? Criteria.where(columnName).isNull().or(columnName).like("%" + property).ignoreCase(ignoreCase)
									: Criteria.where(columnName).like("%" + property).ignoreCase(ignoreCase);
							break;
						case STARTING:
							propertyCriteria = example.getMatcher().getNullHandler() == NullHandler.INCLUDE
									? Criteria.where(columnName).isNull().or(columnName).like(property + "%").ignoreCase(ignoreCase)
									: Criteria.where(columnName).like(property + "%").ignoreCase(ignoreCase);
							break;
						case CONTAINING:
							propertyCriteria = example.getMatcher().getNullHandler() == NullHandler.INCLUDE
									? Criteria.where(columnName).isNull().or(columnName).like("%" + property + "%").ignoreCase(ignoreCase)
									: Criteria.where(columnName).like("%" + property + "%").ignoreCase(ignoreCase);
							break;
						default:
							throw new IllegalStateException(example.getMatcher().getDefaultStringMatcher() + " is not supported!");
					}

					if (example.getMatcher().isAllMatching()) {
						criteria = criteria.and(propertyCriteria);
					} else {
						criteria = criteria.or(propertyCriteria);
					}
				}
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}

		return Query.query(criteria);
	}
}
