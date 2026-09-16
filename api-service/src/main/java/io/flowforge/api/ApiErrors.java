package io.flowforge.api;

import java.util.MissingResourceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiErrors {
    @ExceptionHandler(MissingResourceException.class) public ProblemDetail missing(MissingResourceException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND,e.getMessage());
    }
    @ExceptionHandler(IllegalArgumentException.class) public ProblemDetail invalid(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,e.getMessage());
    }
    @ExceptionHandler(MethodArgumentNotValidException.class) public ProblemDetail validation(MethodArgumentNotValidException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,e.getBindingResult().getFieldErrors().stream()
                .map(error->error.getField()+": "+error.getDefaultMessage()).distinct().sorted().collect(java.util.stream.Collectors.joining("; ")));
    }
    @ExceptionHandler({HttpMessageNotReadableException.class,MethodArgumentTypeMismatchException.class})
    public ProblemDetail malformed(Exception e) { return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,"Malformed JSON or parameter value"); }
}
