package com.tarmac.dispatch.repository;

import com.tarmac.dispatch.domain.IncidentDocument;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface IncidentRepository extends MongoRepository<IncidentDocument, String> {

    List<IncidentDocument> findAllByOrderByReportedAtDesc();

    List<IncidentDocument> findByAirportOrderByReportedAtDesc(String airport);

    List<IncidentDocument> findBySeverityOrderByReportedAtDesc(String severity);
}
