
library(MASS)
head(faithful)

# 1. We first find the range of eruption durations.

duration = faithful$eruptions;
range(duration) 

# 2. Break the range into non-overlapping intervals.

breaks = seq(1.5, 5.5, by=0.5); 
breaks
# 3. Classify the eruption durations according to which interval they fall into.

duration.cut = cut(duration, breaks, right=FALSE)
duration.freq

duration.freq = table(duration.cut);
duration.freq

duration.freq = cbind(duration.freq)
duration.freq

# 4. "Compute the frequency of eruptions in each interval" or count the number of 
#    eruption durations in each interval.

duration.freq = table(duration.cut)
duration.relfreq = duration.freq / nrow(faithful);
duration.relfreq
duration.cut

old = options(digits=3);
cbind(duration.freq, duration.relfreq)

duration = faithful$eruptions; # the eruption
waiting = faithful$waiting; # the waiting interval
head(cbind(duration, waiting));


# plot(duration, waiting, xlab="Eruption duration", ylab="Time waited")
model = lm(waiting ~ duration, data = faithful)
print(model)
plot(duration, waiting, xlab="Eruption duration", ylab="Time waited")
abline(model, col='blue', lwd = 4)

## ----------Problem 3

# By using the function eigen the eigenvalues and eigenvectors of the covariance matrix are computed
Eigenvalues <- eigen(cov(faithful))$values
print(Eigenvalues)
Eigenvectors <- eigen(cov(faithful))$vectors
print(Eigenvectors)

# Prove that two eigin vectors are Orthogonal you multiply them using a Scalar Product 
# scala product of two values should result in 0
# http://hyperphysics.phy-astr.gsu.edu/hbase/vsca.html
# http://www.purplemath.com/modules/mtrxmult.htm

# (A_x * B_x) + (A_y * B_y)
#        [,1]    [,2]
# [1,] 0.0755 -0.9971
# [2,] 0.9971  0.0755
# (0.0755 * 0.9971) + (-0.9971 * 0.0755) = 0 


####### Further Work (outside of problem) ##############################
# http://statmath.wu.ac.at/~hornik/QFS1/principal_component-vignette.pdf
# The Principal Components can be estimated via a matrix multiplication
PC <- as.matrix(faithful) %*% Eigenvectors

# As a check of the result, we compute the covariance matrix of PC. The variances of cov(PC) should
# be equal to the Eigenvalues and the covariances should be 0 (aside from rounding errors) since the
# Principal Components have to be uncorrelated.
cov(PC)

# Eigenvalues[1:2]
Eigenvalues[1:2]

# cov(PC)[1:2, 1:2]
cov(PC)[1:2, 1:2]

# In a next step we calculate the proportions of the variation explained by the various components:
print(round(Eigenvalues/sum(Eigenvalues) * 100, digits = 2))
round(cumsum(Eigenvalues)/sum(Eigenvalues) * 100, digits = 2)

# The first component round(Eigenvalues[1]/sum(Eigenvalues)*100, digits=2) explains 99.87,
# and the first two eigenvectors of the covariance matrix explain 99.9 of the total variation in the data.
# This suggest that the effective dimension of the space of yield curves could be two and any of the yield
# curves from our data set can be described by a linear combination of the first two loadings, while the
# relative error being very small.


## ----------Problem 4
# https://stackoverflow.com/questions/39165340/dataframe-create-new-column-based-on-other-columns
df <- transform(faithful, type= ifelse(eruptions <= 3.1, "short", "long"))

# Checking Results of new column are correct
head(df)

# Making new vectors
waiting = df$waiting
type = df$type

# Creating a new boxplot
par(mfrow=c(1,2))
plot(duration, waiting, xlab="Eruption Duration", ylab="Time Waited", main="Eruption Duration vs Time Waited")
abline(model, col='blue', lwd = 4)
boxplot(waiting ~ type, horizontal = FALSE, xlab="Eruption Duration Type", ylab="Time Waited", main="Eruption Duration Type vs Time Waited")


