
if (!require(random)) install.packages("random")
library(random)
rM <- randomNumbers(n=(40*100),min=-1, max=1, col=40, check=TRUE)
rMPnorm <- pnorm(rM, mean = 0, sd = 0, lower.tail = TRUE, log.p = FALSE)

rMPnorm$new <- rowSums(rMPnorm[,1:40])
rMPnorm$new

hist(rMPnorm$new, prob=TRUE, xlab="Sum", ylab="Frequency", main="Sum Vs Frequency Histogram")
curve(dnorm(x, mean=mean(rMPnorm$new), sd=sd(rMPnorm$new)), add=TRUE)