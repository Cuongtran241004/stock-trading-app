package com.trading.market_data_service.controller;

import com.trading.market_data_service.dto.StockPrice;
import com.trading.market_data_service.entity.Stock;
import com.trading.market_data_service.service.MarketDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/market")
@Slf4j
@RequiredArgsConstructor
public class MarketDataController {
    private final MarketDataService marketDataService;

    /**
     * Get all stocks with current pricess
     * @return
     */
    @GetMapping("/stocks")
    public ResponseEntity<List<StockPrice>> getAllPrices() {
        return ResponseEntity.ok(
                marketDataService.getAllPrices()
        );
    }


    /**
     * Get current price for specific stock
     * @param symbol
     * @return
     */
    @GetMapping("/stocks/{symbol}")
    public ResponseEntity<StockPrice> getStockPrice(
            @PathVariable String symbol
    ){
        return ResponseEntity.ok(
                marketDataService.getPrice(
                        symbol.toUpperCase()
                )
        );
    }

    /**
     * Get all stocks
     * Shows which stocks are available for trading.
     * @return
     */
    @GetMapping("/stocks/list")
    public ResponseEntity<List<Stock>> getStockList(){
        return ResponseEntity.ok(
                marketDataService.getAllStocks()
        );
    }

}
