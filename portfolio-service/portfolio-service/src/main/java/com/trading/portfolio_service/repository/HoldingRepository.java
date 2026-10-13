package com.trading.portfolio_service.repository;

import com.trading.portfolio_service.entity.Holding;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface HoldingRepository extends JpaRepository<Holding, String> {
    List<Holding> findByUserId(String userId);

    Optional<Holding> findByUserIdAndSymbol(String userId, String symbol);
}
