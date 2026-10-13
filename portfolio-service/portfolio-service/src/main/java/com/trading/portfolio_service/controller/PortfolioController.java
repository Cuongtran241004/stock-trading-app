package com.trading.portfolio_service.controller;

import com.trading.portfolio_service.service.PortfolioService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/api/v1/portfolio")
@RequiredArgsConstructor
public class PortfolioController {
    private final PortfolioService portfolioService;


    /**
     * Get my portfolio with live P&L
     * @param userId
     * @return
     */
    @GetMapping("/me")
    public ResponseEntity<Map<String, Object>> getMyPortfolio(
            @RequestHeader("X-User-Id") String userId
    ){
        return ResponseEntity.ok(
                portfolioService.getPortfolio(userId));
    }


    /**
     * For portfolio by userId - For internal calls
     * @param userId
     * @return
     */
    @GetMapping("/{userId}")
    public ResponseEntity<Map<String, Object>> getPortfolio(
            @PathVariable String userId
    ){
        return ResponseEntity.ok(portfolioService.getPortfolio(userId));
    }
}
