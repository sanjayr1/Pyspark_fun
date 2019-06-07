# Pyspark projects

# Program 1
- Subdivide geographic data on a 2D grid, and process each grid cell separately or in conjunction with nearby grid cells
- Maps each input point to the cell that contains it, along with the 4 adjacent cells, 3 below and 1 to the right
- Process the data from each grid cell and find allpairs of points within a specified distance
- Give Average, Max, and Min distance over all pairs of points

# Program 2
- Given historical stock prices from the NYSE S&P 500 stocks, and data on company fundamentals, analyze the performance of each stock over the time period (2010 to 2016)
- For each ticker symbol in the fundamentals data, compute the average of all occurrences of the "Estimated Shares Outstanding" values for all time periods
- For each ticker symbol, find the mean, min, max, and variance of the "close" value of each stock over all the price data
-  For each ticker symbol, also find the mean, min, max, and variance of each stock valuation based on the "close" prices for each day, and using the average of "Estimated Shares Outstanding" 
- Save output as (ticker,pMean pMin pMax pVar vMean vMin vMax vVar)
- Count and discard data for stock tickers occurring in the price data but not in the fundamentals data, and vice versa
- Count the total number of stock price records read, and the total number of fundamentals records read

# Program 3
- Implement a Pregel GraphFrames algorithm to calculate the single-source shortest path from one of the nodes in a graph to every other node
- Implement a Pregel GraphFrames algorithm to calculate the connected components of a given graph
