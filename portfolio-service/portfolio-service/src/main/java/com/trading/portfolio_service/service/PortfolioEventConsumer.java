package com.trading.portfolio_service.service;


import com.trading.portfolio_service.entity.Holding;
import com.trading.portfolio_service.repository.HoldingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class PortfolioEventConsumer {

    private final HoldingRepository holdingRepository;

    @KafkaListener(topics = "order.executed")
    public void consumeOrderExecuted(
            @Payload Map<String, Object> payload
            ){
        try {
            String userId = (String) payload.get("userId");
            String symbol = (String) payload.get("symbol");
            String type = (String)  payload.get("type");

            int quantity = Integer.parseInt((String) payload.get("quantity"));
            BigDecimal price = (BigDecimal) payload.get("price");

            log.info("Updating portfolio - user: {} {} x {} at {}",
                    userId, symbol, quantity, price);


            if("BUY".equals(type)){
                addHolding(userId, symbol, quantity, price);

            }else if("SELL".equals(type)){
                removeHolding(userId, symbol, quantity);
            }
        } catch (Exception e) {
            log.error("Error updating portfolio: {}", e.getMessage());
        }
    }

    /**
     * Add or update after BUY order
     * Calculates new average buy price
     * @param userId
     * @param symbol
     * @param quantity
     * @param price
     */
    private void addHolding(String userId, String symbol, int quantity, BigDecimal price) {
        Holding holding = holdingRepository
                .findByUserIdAndSymbol(userId, symbol)
                .orElse(null);

        if(holding == null){
            // First time buying this stock
            holding = new Holding();
            holding.setUserId(userId);
            holding.setSymbol(symbol);
            holding.setCompanyName(symbol);
            holding.setQuantity(quantity);
            holding.setAverageBuyPrice(price);
            holding.setTotalInvested(
                    price.multiply(BigDecimal.valueOf(quantity))
            );
        }else{
            // Already holding this stock - Update average price
            int newQuantity = holding.getQuantity() + quantity;
            BigDecimal newInvested = holding.getTotalInvested()
                    .add(price.multiply(BigDecimal.valueOf(quantity)));

            BigDecimal newAverageBuyPrice = newInvested
                    .divide(BigDecimal.valueOf(newQuantity), 2, RoundingMode.HALF_UP);

            holding.setQuantity(newQuantity);
            holding.setTotalInvested(newInvested);
            holding.setAverageBuyPrice(newAverageBuyPrice);
        }


        holdingRepository.save(holding);
        log.info("Holding updated: {} {} shared of {}",
                userId, holding.getQuantity(), symbol);

    }

    /**
     *
     * @param userId
     * @param symbol
     * @param quantity
     */
    private void removeHolding(String userId, String symbol, int quantity) {
        Holding holding = holdingRepository
                .findByUserIdAndSymbol(userId, symbol)
                .orElseThrow(() -> new RuntimeException(
                        "Holding not found: " + symbol
                ));

        if(holding.getQuantity() < quantity){
            throw new RuntimeException(
                    "Insufficient shares to SELL");
        }

        int newQuantity = holding.getQuantity() - quantity;

        if(newQuantity == 0){
            holdingRepository.delete(holding);
            log.info("Holding removed: {} sold all {} shares",
                    userId, symbol);
        }else{
            BigDecimal newInvested = holding.getAverageBuyPrice()
                    .multiply(BigDecimal.valueOf(newQuantity));

            holding.setQuantity(newQuantity);
            holding.setTotalInvested(newInvested);

            holdingRepository.save(holding);

            log.info("Holding reduced: {} now has {} shares of {}",
                    userId, newQuantity, symbol);
        }
    }
}
