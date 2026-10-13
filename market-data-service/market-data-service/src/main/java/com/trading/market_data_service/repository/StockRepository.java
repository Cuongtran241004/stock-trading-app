package com.trading.market_data_service.repository;

import com.trading.market_data_service.entity.Stock;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface StockRepository extends JpaRepository<Stock, String> {
    List<Stock> findByActiveTrue();
}
