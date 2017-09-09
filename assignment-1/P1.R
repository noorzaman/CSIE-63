setwd("/Users/swaite/stirling/CSIE-63/assignment-1/")

options(scipen = 999)

x <- seq(0, 60, by=1)
yP30 <- dbinom(x, 60, 0.3)
yP50 <- dbinom(x, 60, 0.5)
yP80 <- dbinom(x, 60, 0.8)

plot(x, yP30)
plot(x, yP50)
plot(x, yP80)

df <- data.frame(P30=numeric(),
                 P50=numeric(), 
                 P80=numeric(), 
                 stringsAsFactors=FALSE)

df["1st_quar",]   = c(quantile(yP30)[2]["25%"], quantile(yP50)[2], quantile(yP80)[2])
df["median",]     = c(quantile(yP30), quantile(yP50), quantile(yP80))
df["mean",]       = c(mean(yP30), mean(yP50), mean(yP80))
df["stdev",]      = c(sd(yP30), sd(yP50), sd(yP80))
df["3rd_quar",]   = c(quantile(yP30)[4], quantile(yP50)[4], quantile(yP80)[4])
print(df)

boxplot(df, horizontal = FALSE)

  
  
## Old Answer

df <- data.frame(P30=numeric(),
                 P50=numeric(), 
                 P80=numeric(), 
                 stringsAsFactors=FALSE) 




bp_df <- data.frame(P30=numeric(),
                    P50=numeric(), 
                    P80=numeric(), 
                    stringsAsFactors=FALSE)

getBinomialDist = function(){
  for (i in 1:60){
    df[nrow(df) + 1,] = c(
                          dbinom(i, size=60, prob=0.3),
                          dbinom(i, size=60, prob=0.5),
                          dbinom(i, size=60, prob=0.8)
                        )
  }
  return(df)
}
df = getBinomialDist()

print(df)
plot(df$P30)

getStatsData = function(df){
  bp_df["1st_quar",] = c(summary(df$P30)[2], summary(df$P50)[2], summary(df$P80)[2])
  bp_df["median",] = c(summary(df$P30)[3], summary(df$P50)[3], summary(df$P80)[3])
  bp_df["mean",] = c(summary(df$P30)[4], summary(df$P50)[4], summary(df$P80)[4])
  bp_df["stdev",] = apply(df,2,sd) #all complete
  bp_df["3rd_quar",] = c(summary(df$P30)[5], summary(df$P50)[5], summary(df$P80)[5])
  return(bp_df)
}

getDataPlots = function(df, bp_df){
  plot(df)
  boxplot(bp_df, horizontal = FALSE)
  return()
}

df = getBinomialDist()
print(df)
bp_df = getStatsData(df)
print(bp_df)
getDataPlots(df, bp_df)







