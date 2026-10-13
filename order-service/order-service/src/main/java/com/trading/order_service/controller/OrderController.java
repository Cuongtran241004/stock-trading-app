package com.trading.order_service.controller;

import com.trading.order_service.dto.OrderRequest;
import com.trading.order_service.entity.Order;
import com.trading.order_service.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/orders")
@Slf4j
@RequiredArgsConstructor
public class OrderController {
    private final OrderService orderService;

    /**
     * Place buy or sell order
     */
    @PostMapping
    public ResponseEntity<Order> placeOrder(
            @RequestHeader("X-User-Id") String userId,
            @Valid @RequestBody OrderRequest request
    ) {
        request.setUserId(userId);

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(orderService.placeOrder(request));
    }


    /**
     *
     * @param orderId
     * @param userId
     * @return
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(
            @PathVariable String orderId,
            @RequestHeader("X-User-Id") String userId
    ){
        Order order = orderService.getOrder(orderId);

        // Security Check
        if(!order.getUserId().equals(userId)){
            return ResponseEntity.status(403).build();
        }

        return ResponseEntity.ok(order);
    }


    @GetMapping("/my-orders")
    public ResponseEntity<List<Order>> getMyOrders(
            @RequestHeader("X-User-Id") String userId){
        return ResponseEntity.ok(
                orderService.getMyOrders(userId)
        );
    }
}
