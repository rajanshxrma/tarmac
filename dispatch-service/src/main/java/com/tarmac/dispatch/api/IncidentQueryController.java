package com.tarmac.dispatch.api;

import com.tarmac.dispatch.domain.DeadLetterDocument;
import com.tarmac.dispatch.domain.IncidentDocument;
import com.tarmac.dispatch.repository.DeadLetterRepository;
import com.tarmac.dispatch.repository.IncidentRepository;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class IncidentQueryController {

    private final IncidentRepository incidentRepository;
    private final DeadLetterRepository deadLetterRepository;

    public IncidentQueryController(IncidentRepository incidentRepository,
            DeadLetterRepository deadLetterRepository) {
        this.incidentRepository = incidentRepository;
        this.deadLetterRepository = deadLetterRepository;
    }

    @GetMapping("/incidents")
    public List<IncidentDocument> incidents(@RequestParam(required = false) String airport,
            @RequestParam(required = false) String severity) {
        if (airport != null && !airport.isBlank()) {
            return incidentRepository.findByAirportOrderByReportedAtDesc(airport.trim().toUpperCase());
        }
        if (severity != null && !severity.isBlank()) {
            return incidentRepository.findBySeverityOrderByReportedAtDesc(severity.trim().toUpperCase());
        }
        return incidentRepository.findAllByOrderByReportedAtDesc();
    }

    @GetMapping("/incidents/{id}")
    public ResponseEntity<IncidentDocument> incident(@PathVariable String id) {
        return incidentRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/deadletters")
    public List<DeadLetterDocument> deadLetters() {
        return deadLetterRepository.findTop20ByOrderByFailedAtDesc();
    }
}
