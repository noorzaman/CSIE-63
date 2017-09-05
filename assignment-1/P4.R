library(MASS)
head(faithful)

# https://stackoverflow.com/questions/39165340/dataframe-create-new-column-based-on-other-columns
df <- transform(faithful, type= ifelse(eruptions <= 3.1, "short", "long"))

# Checking Results of new column are correct
head(df)

# Making new vectors
waiting = df$waiting
type = df$type

# Creating a new boxplot
boxplot(waiting ~ type, horizontal = FALSE)