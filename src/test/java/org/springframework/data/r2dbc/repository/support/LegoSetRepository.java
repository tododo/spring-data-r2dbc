package org.springframework.data.r2dbc.repository.support;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LegoSetRepository extends R2dbcRepository<AbstractSimpleR2dbcRepositoryIntegrationTests.LegoSet, Integer> {

}
