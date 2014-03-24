##
#  Accompaniment file for the R script fs.R
##

require(e1071)

# original version 
SVMRFE = function(x,y){
  n = ncol(x)
  survivingFeaturesIndexes = seq(1:n) 
  featureRankedList = vector(length=n) 
  rankedFeatureIndex = n
  while(length(survivingFeaturesIndexes)>0){ 
    #train the support vector machine s
    svmModel = svm(x[, survivingFeaturesIndexes], y, cost = 1, cachesize=500, scale=F, type="C-classification", kernel="linear" )
    #compute the weight vector
    w = t(svmModel$coefs)%*%svmModel$SV
    #compute ranking criteria
    rankingCriteria = w * w
    #rank the features 
    ranking = sort(rankingCriteria, index.return = TRUE)$ix
    #update feature ranked list 
    featureRankedList[rankedFeatureIndex] = survivingFeaturesIndexes[ranking[1]] 
    rankedFeatureIndex = rankedFeatureIndex - 1
    #eliminate the feature with smallest ranking criterion 
    (survivingFeaturesIndexes = survivingFeaturesIndexes[-ranking[1]])
  } 
  return (featureRankedList)
}



##
# SVM RFE: microarray version, maximally for cca 130.000  features
# best < number of features
##
# IN: x -- dataset (samples in rows)
#     y -- factor  
#     best -- parameter for SVM-RFE
# OUT: new order -- the best is the first
#
RestrictedSVMRFE <- function(x, y, best=NA){
  #
  n <- ncol(x)
  if(is.na(best)) {best <- n}
  if(best<2 | best>n) stop("Argument 'best' is out of its range.")
  #
  survivingFeaturesIndexes <- seq(1:n)
  featureRankedList <- vector(length = n, mode="numeric")
  rankedFeatureIndex <- best
  # generate selecting sequence
  seq.remain <-  c( 0, 1:best, best+2^(0:log(n-best+1,2)) )
  seq.remain <-  seq.remain[seq.remain < n]
  i <- length(seq.remain)  # actual number of features which survive to the next iteration
  count <- 0
  while( i > 0 ){
    count <- count+1
    #train the support vector machine
    svmModel <- svm(x[, survivingFeaturesIndexes], y, cost = 1, cachesize=500, scale=F, type="C-classification", kernel="linear" )
    #compute the weight vector
    w <- t(svmModel$coefs)%*%svmModel$SV
    #compute ranking criteria
    rankingCriteria <- w * w
    #rank the features
    ranking.rel <- sort(rankingCriteria, index.return = TRUE, decreasing = T)$ix
    ranking.abs <- survivingFeaturesIndexes[ranking.rel]
    #update feature ranked list
    if(seq.remain[i] < best) {
      survivingFeaturesIndexes <- setdiff(survivingFeaturesIndexes, tail(ranking.abs,1))
      featureRankedList[rankedFeatureIndex] <- tail(ranking.abs,1)
      rankedFeatureIndex <- rankedFeatureIndex - 1
    }else{
      survivingFeaturesIndexes <- intersect(survivingFeaturesIndexes, ranking.abs[1:seq.remain[i]])
      removedFeatures <- setdiff(ranking.abs,  survivingFeaturesIndexes)
      index <- ranking.abs %in% removedFeatures
      removedFeaturesRightOrder <- ranking.abs[index]
      featureRankedList[(seq.remain[i]+1):(seq.remain[i]+length(removedFeatures))] <- removedFeaturesRightOrder
    }
    i <- i-1
  }
  return (featureRankedList)
}


##
# IN: x -- dataset (samples in rows)
#     y -- factor
#     best -- number of the top features to be returned
# OUT: new order -- the best is the first
#
TTestRanking <- function(x, y, best=NA, regc = 0) {
  
  n <- ncol(x)
  if(is.na(best)) {best <- n}
  if(best<1 | best>n) stop("Argument 'best' is out of its range.")
  #
  t.stat <- apply(x, 2, function(X){
    X.mean <- tapply(X, y, mean)
    X.var  <- tapply(X, y, var)
    X.size <- tapply(X, y, length)
    t.score <- (X.mean[1]-X.mean[2])/sqrt(regc+sum(X.var/X.size))
    t.score
  })
  t.stat[is.nan(t.stat)] <- 100 # avoid falling because of NaN invoked by the t-test
  abs.t.stat.soreted <- sort(abs(t.stat), index.return = TRUE, decreasing=T)$ix 
  abs.t.stat.soreted[1:best]
}

##
# IN: x -- dataset (samples in rows)
#     y -- factor
#     best -- number of the top features to be returned
# OUT: new order -- random set of features (row positions -- integers) of size best
#
RandomRanking <- function(x, y, best=NA) {  
        #
        n <- ncol(x)  
        if(is.na(best)) {best <- n}
        if(best<1 | best>n) stop("Argument 'best' is out of its range.")
        #
        sample(1:n, best, replace=F)
}


