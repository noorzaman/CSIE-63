setwd("/Users/swaite/stirling/CSIE-63/assignment-1/")

options(scipen = 999)

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
                          pbinom(i, size=60, prob=0.3),
                          pbinom(i, size=60, prob=0.5),
                          pbinom(i, size=60, prob=0.8)
                        )
  }
  return(df)
}

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







