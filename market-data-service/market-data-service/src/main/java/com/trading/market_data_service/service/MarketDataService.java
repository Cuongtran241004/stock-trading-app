package com.trading.market_data_service.service;

import com.trading.market_data_service.dto.StockPrice;
import com.trading.market_data_service.entity.Stock;
import com.trading.market_data_service.repository.StockRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
@Order(2)
public class MarketDataService implements CommandLineRunner {
//    @Override
//    public void run(String... args) throws Exception {
//
//    }

    private final StockRepository stockRepository;

    private final Random random = new Random();

    private final RedisTemplate<String, Object> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;

    private static final String PRICE_KEY_PREFIX = "stock:price:";
    private static final String PRICE_UPDATED_TOPIC = "stock.price.updated";


    /**
     * ConcurrentHashMap - for thread safety
     */
    private final Map<String, StockPrice> stockPrices = new ConcurrentHashMap<>();


    /**
     * Initialize Stock Prices
     *
     * @PostConstruct -> runs when context is ready
     */
    @Override
    public void run(String...args){
        log.info("Loading stock data ...");

        List<Stock> stocks = stockRepository.findByActiveTrue();

        if(stocks.isEmpty()){
            log.warn("No stocks found");
            return;
        }


        stocks.forEach(stock -> {
            StockPrice stockPrice = new StockPrice();
            stockPrice.setSymbol(stock.getSymbol());
            stockPrice.setCompanyName(stock.getCompanyName());
            stockPrice.setPrice(stock.getInitialPrice());
            stockPrice.setOpen(stock.getInitialPrice());
            stockPrice.setHigh(stock.getInitialPrice());
            stockPrice.setLow(stock.getInitialPrice());
            stockPrice.setChange(BigDecimal.ZERO);
            stockPrice.setChangePercent(BigDecimal.ZERO);
            stockPrice.setVolume(0L);
            stockPrice.setTimestamp(LocalDateTime.now());

            stockPrices.put(stock.getSymbol(), stockPrice);

            log.info("Loaded Stock: {} at {}", stock.getSymbol(), stock.getInitialPrice());
        });


        log.info("Finished loading {} stock data", stockPrices.size());
    }


    /**
     * Scheduled price update - every 2 seconds
     */
    @Scheduled(fixedRateString = "${market.price-update-interval}")
    public void updatePrices(){
        if(stockPrices.isEmpty()){
            log.warn("No stocks found");
            return;
        }

        stockPrices.forEach((symbol, currentPrice) -> {
            BigDecimal oldPrice = currentPrice.getPrice();


            // +- price movement
            double priceMovement = (random.nextDouble() - 0.5) * 0.5;

            BigDecimal priceChange = oldPrice
                    .multiply(BigDecimal.valueOf(priceMovement / 100))
                    .setScale(2, RoundingMode.HALF_UP);

            BigDecimal newPrice = oldPrice
                    .add(priceChange)
                    .setScale(2, RoundingMode.HALF_UP);


            // Prevent negative price
            if (newPrice.compareTo(BigDecimal.valueOf(1)) < 0) {
                newPrice = BigDecimal.valueOf(1);
            }


            BigDecimal changeAmount = newPrice
                    .subtract(currentPrice.getOpen())
                    .setScale(1, RoundingMode.HALF_UP);

            BigDecimal changePercent = changeAmount
                    .divide(currentPrice.getOpen())
                    .multiply(BigDecimal.valueOf(100))
                    .setScale(2, RoundingMode.HALF_UP);



            // Update High - Low
            BigDecimal newHigh = newPrice
                    .compareTo(currentPrice.getHigh()) > 0
                    ? newPrice : currentPrice.getHigh();

            BigDecimal newLow = newPrice
                    .compareTo(currentPrice.getLow()) < 0
                    ? newPrice : currentPrice.getLow();


            currentPrice.setPrice(newPrice);
            currentPrice.setChange(changeAmount);
            currentPrice.setChangePercent(changePercent);
            currentPrice.setHigh(newHigh);
            currentPrice.setLow(newLow);
            currentPrice.setTimestamp(LocalDateTime.now());


            // 1. Cache in Redis
            redisTemplate.opsForValue()
                    .set(PRICE_KEY_PREFIX + symbol, currentPrice);

            // 2. Broadcast via WebSocket -> all clients
            messagingTemplate.convertAndSend(
                    "/topic/prices" + symbol, currentPrice
            );

            // 3. Publish to Kafka
            Map<String, Object> priceUpdatedEvent = new HashMap<>();
            priceUpdatedEvent.put("symbol", symbol);
            priceUpdatedEvent.put("price", newPrice);
            priceUpdatedEvent.put("timestamp", LocalDateTime.now().toString());

            kafkaTemplate.send(PRICE_UPDATED_TOPIC, symbol, priceUpdatedEvent);
        });

        log.debug("Prices updated for {} stocks", stockPrices.size());
    }


    public StockPrice getPrice(String symbol) {
        Object cached = redisTemplate
                .opsForValue()
                .get(PRICE_KEY_PREFIX + symbol);

        if (cached != null) {
            return (StockPrice) cached;
        }

        // Fallback
        StockPrice price = stockPrices.get(symbol.toUpperCase());
        if (price == null) {
            throw new RuntimeException("No stock found for symbol: " + symbol);
        }

        return price;
    }


    /**
     * Get all active stock prices
     * @return
     */
    public List<StockPrice> getAllPrices(){
        return stockPrices.values().stream().toList();
    }

    /**
     * Get all stocks
     * @return
     */
    public List<Stock> getAllStocks(){
        return stockRepository.findByActiveTrue();
    }

    private StockPrice mapToResponse(Stock stock){
        StockPrice stockPrice = new StockPrice();



        return stockPrice;
    }
}
