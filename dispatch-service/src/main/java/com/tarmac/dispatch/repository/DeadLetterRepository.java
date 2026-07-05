package com.tarmac.dispatch.repository;

import com.tarmac.dispatch.domain.DeadLetterDocument;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface DeadLetterRepository extends MongoRepository<DeadLetterDocument, String> {

    List<DeadLetterDocument> findTop20ByOrderByFailedAtDesc();
}
