### testovaci soubor


test <- function(x, ...) UseMethod("test")

test.default <- function(x, ...) {

  retval <- list()
  retval$sum <- sum(x)
  retval$nrow <- nrow(x)
  retval$ncol <- ncol(x)
  retval$call <- match.call()
  retval$x <- x
  
  class(retval) <- "test"
  retval
}


summary.test <- function(object, ...) {
  retval <- list()
  tab <- cbind(sum=object$sum,
               nrow=object$nrow,
               ncol=object$ncol) 
  retval <- list(call=object$call, cf=tab)  
  
  class(retval) <- "summary.test"
  retval
}

print.summary.test <- function(x, ...) {
  cat("Call:\n")
  print(x$call)
  cat("\n")
  print(x$cf)  
}
