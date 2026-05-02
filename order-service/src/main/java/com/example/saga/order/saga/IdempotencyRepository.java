package com.example.saga.order.saga;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

// ── Entity ────────────────────────────────────────────────────────────────────

@Entity
@Table(name = "idempotency_records")
@Getter
@NoArgsConstructor
class IdempotencyRecord {
    @Id @GeneratedValue(strategy = GenerationType.UUID) UUID id;
    @Column(name = "idempotency_key", unique = true, nullable = false) String idempotencyKey;
    @Column(name = "processed_at") LocalDateTime processedAt = LocalDateTime.now();
    IdempotencyRecord(String key) { this.idempotencyKey = key; }
}

// ── Spring Data Repository ────────────────────────────────────────────────────

@Repository
interface IdempotencyJpaRepository extends JpaRepository<IdempotencyRecord, UUID> {
    Optional<IdempotencyRecord> findByIdempotencyKey(String key);
}

// ── Service ───────────────────────────────────────────────────────────────────

@Repository
class IdempotencyRepository {

    private final IdempotencyJpaRepository repo;

    IdempotencyRepository(IdempotencyJpaRepository repo) { this.repo = repo; }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean alreadyProcessed(String key) {
        return repo.findByIdempotencyKey(key).isPresent();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markProcessed(String key) {
        try { repo.save(new IdempotencyRecord(key)); }
        catch (Exception ignored) { /* unique constraint = already marked */ }
    }
}
