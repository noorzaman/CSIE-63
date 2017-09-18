

# Problem 5
df <- data.frame(replicate(40, runif(100, min = -1, max = 1)))
hist(df$X1)
hist(df$X40)

# Problem 6
df$sum <- rowSums(df)
print(df)

x_norm <- seq(min(df$sum), max(df$sum), length=20)
y_norm <- (dnorm(xfit, mean=mean(df$sum), sd=sd(df$sum))) * (diff(h$mids[1:2]*length(df$sum)))
lines(x_norm, y_norm, col="blue", lwd=2)

h <- hist(df$sum, breaks = 10, col="yellow", main = "Histogram with Gaussian Curve", xlab="Sum", ylab="Frequency")
lines(x_norm, y_norm, col="purple", lwd=2)