package com.trading.market_data_service.config;

import com.trading.market_data_service.entity.Stock;
import com.trading.market_data_service.repository.StockRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
@Order(1)
public class DataInitializer implements CommandLineRunner {
    private final StockRepository stockRepository;

    @Override
    public void run(String... args) throws Exception {
        if(stockRepository.count() > 0){
            log.info("Stock has been initialized");
            return;
        }

        log.info("Initializing stock data ...");
        List<Stock> stocks = List.of(
                new Stock("AAPL", "Apple Inc", BigDecimal.valueOf(189.50),
                  "NASDAQ", "USD", true),
                new Stock("GOOGL", "Google Inc", BigDecimal.valueOf(141.50),
                        "NASDAQ", "USD", true),
                new Stock("MSFT", "microsoft Inc", BigDecimal.valueOf(182.50),
                        "NASDAQ", "USD", true)
                );

        stockRepository.saveAll(stocks);
        log.info("Stock has been initialized", stocks.size());
    }
}
