rm(list=ls())
#.rs.restartR()
gc()

set.seed(123)
#Automatically detach all packages R
#invisible(lapply(paste0('package:', names(sessionInfo()$otherPkgs)), detach, character.only=TRUE, unload=TRUE))

#week_end <- getArgument("week_end","PARAMS")
#week_end <- as.integer(week_end)

Time_tot_s = Sys.time()



#connection retry function
contry <- function(tsk,attempts, sleep.seconds ,Query) {

  if (tsk=='load'){ 
    for (i in 1:attempts) {
      df <- try(dataset.load(name = 'GCP DD', query = Query), silent = TRUE)
      if (!("try-error" %in% class(df))) {
        print("Connected...")
        return(df)
      }
      else {
        print(paste0("Attempt #", i, " failed"))
        Sys.sleep(sleep.seconds)
      }
    }
    stop("Maximum number of connection attempts exceeded")
  }
}

#Take backup of current table


#Drop HDFS previous data
# connector.execute(name="HDFS_Prod17", statement="hadoop fs -rm -r /user/hive/warehouse/dbcure.db/Weekly_Scorecard/PullForward/PullForward_auto/*")
# connector.execute(name="HDFS_Prod17", statement="hadoop fs -rm -r /user/svccaiaproddse/Weekly_Scorecard_backup/PullForward/PullForward_auto/*")
#contry('drop',10,5,NA)

#Automatically dropping all files
f1 <- list.files('/data/GCP/PF_Corr/PF_auto/', recursive = TRUE, full.names = TRUE, include.dirs = FALSE)
file.remove(f1)

#Modelling Libraries
library(doParallel)#Parallelization
library(foreach)#Parallelization
library(igraph)#Baseline function
library(dplyr)#CorFor
library(tidyr)#CorFor
library(Matrix)#Baseline function
library(car)#VIF calculation/Baseline function
library(ranger)#RF fit/Baseline function

#Data Preperation 
library(mlutils)
library(strucchange)
library(Kendall)
library(purrr)
library(caret)
library(data.table)#Baseline function
setDTthreads(threads = 1)

#Novel method to decompose a level shifted time series 
#Library import
library(strucchange)
library(ggplot2)
library(tseries)
library(data.table)
library(digest)
library(Kendall)
#Version checking

#' Multi Level STL decomposition
#'
#' @param Data series data that is longer than at leaset two periods. Fed as
#'   single vector
#' @param frequency Frequency of the series
#' @param break_level minimal segment size either given as fraction relative to
#'   the sample size or as an integer giving the minimal number of observations
#'   in each segment. Used to identify breakpoints , higher values will result
#'   in more breakpoints.
#' @param mean_check Minimum mean difference of four points between two levels.
#'   Used to identify sharp level shifts compared to a gradual shift
#' @param median_level Minimum median ratio between two levels to be considered
#'   as a level shift
#' @param conf_level Value of the MAD multiplier that is used to set the
#'   confidence interval to detect anomalies
#' @param window_len Minumum number of points that are taken to identify the
#'   median of a section. A combination of windows is used to derive the
#'   underlying trend of the series, which is a useful baseline to identify
#'   anomalies
#' @param plot If set to TRUE, returns the outliers detected
#' @param level_length Minimum length to identify a level
#'
#' @return The decomposed time series components, inculding the trend ,
#'   seasonality, residuals , smoothend trend of the series and a plot of the
#'   anomalies
#' @export
#'
#' @examples
#' new_series <- ds_series(data_input, frequency = 52, break_level = 1.7, mean_check = 1.5, median_level = 1.5, conf_level = 0.1, window_len = 14, plot = TRUE)

#Changes needed:
#For some reason , the trend takes into account leading zeros into final result , explore standard STL behavior?

ds_series_new <- function(Data, frequency = 52, break_level = 0.05, mean_check = 1.5, median_level = 1.5,level_length = 20, conf_level = 0.1, window_len = 14, mk_test = FALSE,  Alpha = 0.01 , tau = 0.8, plot = FALSE)
{
  
  #Writing the starting data to another value
  y <- Data
  
  #Definition of external vectors
  anomalies <- vector()
  
  #Inital Sanity checks
  if(!is.numeric(frequency) || !is.numeric(break_level) || !is.numeric(mean_check) || !is.numeric(median_level) || !is.numeric(conf_level)
     || !is.numeric(window_len))
  {stop(print('Value needs to be numeric'))}
  
  if(!is.logical(plot))
  {stop(print('Value needs to be boolean'))}
  
  #If contains NA break it
  if(any(is.na(t)))
  {stop(print('Interpolation of NA values needed, recommend the zoo package'))}
  
  #Remvoving the leading and lagging zeros
  non_zero_length <- y[min(which(y!=0 )) : length(y)]
  
  #updating the frame based on the new length
  z <- (length(y)- length(non_zero_length)) +1
  y <- y[c(z:length(y))]
  
  #Strucchange values
  mvalue <- NA
  bp <- NA
  
  tryCatch(
    {
      mvalue = breakpoints(y~1, h = break_level)
    }, error = function(e){bp <<- NA}
    
  )
  
  if(!(is.na(mvalue)))
  {bp = mvalue$breakpoints}
  
  if(is.na(bp)){print('Change break value, min segment size must be larger than number of regressors')}
  
  #Writing the breakpoints
  t<- vector()
  if(is.na(bp))
  {
    t<- c(0,length(y))
  }else
    t<- c(0,bp,length(y))
  
  #Seasonal Check for Level changes
  difference_mat <- outer(t,t,'-')
  difference_table <- which(difference_mat == frequency, arr.ind = TRUE)
  difference_table <- data.frame(difference_table)
  
  p1 <- vector()
  p2 <- vector()
  
  if(dim(difference_table)[1] != 0)
  {
    for(l in seq(from = 1, to = c(dim(difference_table)[1]), by = 1))
    {
      p1 <- c(t[difference_table['row'][l,]], t[difference_table['col'][l,]])
      p2 <- c(p2,p1)
    }
  }
  
  t <- t[!(t %in% p2)]
  
  
  #Median Method to clean breakpoints
  med_flag <- FALSE
  n <- length(y)
  k <- vector()
  i <- 1
  j <- 3
  
  while(i<n)
  {
    while(j < n+1)
    {
      L <- t[i]
      R <- t[j]
      Lr <- t[i+1]
      
      if((is.na(R)))
      {
        med_flag <- TRUE
        break
      }
      
      mid1 <- median(y[c(L+1) : c(Lr)])
      mid2 <- median(y[c(Lr +1) : (R)])
      
      if(mid2 == 0)
      {
        mid2 <- 0.0000001
      }
      
      if((abs(mid1/mid2) > median_level) || abs(mid2/mid1 > median_level))
      {
        k <- c(k, Lr)
        break
      }else{
        t <- t[t!=Lr]
      }
    }
    
    i <- i+1
    j <- j+1
    L <- t[i]
    
    if(med_flag == TRUE)
    {break}
  }
  
  i<- vector()
  j <- vector()
  
  
  #Mean point check
  q <- vector()
  for(x in seq(0,c(length(k)), 1))
  {
    if(x == length(k))
    {
      break
    }else{
      before = y[c(k[[x+1]]- 4):c(k[[x+1]]-1)]
      rownames(before) <- NULL
      
      after  = y[c(k[[x+1]]+1): c(k[[x+1]]+ 4)]
      rownames(after) <- NULL
      
      #Getting the mean values
      M1 = mean(before)
      M2 = mean(after)
      
      #Checking to see mean ratio
      if((abs(M1/M2))>mean_check || ((abs(M2/M1)) > mean_check))
      {
        q<- c(q, k[x+1])
      }
    }
  }
  
  #minimum level length check
  q <- c(0,q,length(y))
  
  if(length(q != 2))
  {
    breaks <- vector()
    for( i in seq(from = 1, to = c(length(q)-1), by = 1))
    {
      if((q[i+1]-q[i]) >=level_length)
      {
        breaks <- c(q[i+1], breaks)
      }
    }
    
    breaks<- unique(sort(breaks))
    breaks <- c(0, breaks , length(y))
    
    if(length(breaks)==2)
    {break}else if(c(length(y) - tail(breaks,2)[1])<= level_length)
    {breaks <- breaks[-match(tail(breaks,2)[1], breaks)]}
    
    q <- breaks
    
  }else{
    q <- c(0,q,length(y))
  }
  
  #Mann Kendall test
  if(mk_test == TRUE)
  {
    mk <- vector()
    mk_flag <- FALSE
    
    for(g in seq(2,c(length(q)),1))
    {
      if(length(y) == q[g]){break}
      
      if(mk_flag == FALSE)
      {
        current <- median(y[c(q[g]+1) : q[g+1]])
        previous <- median(y[c(q[g-1]+1) : q[g]])
      }
      
      if(current > previous)
      {
        Mank <- MannKendall(y[c(q[g]+1) : q[g+1]])
        pval <- Mank$sl
        Mtau <- Mank$tau
        
        if((Mtau < c(-tau)) || (pval > Alpha))
        {
          mk <- c(mk, q[g])
        }
      }else if(previous > current)
      {
        Mank <- MannKendall(y[c(q[g]+1) : q[g+1]])
        pval <- Mank$sl
        Mtau <- Mank$tau
        
        if((Mtau < c(tau)) || (pval > Alpha))
        {
          mk <- c(mk, q[g])
        }
      }
      
      if(mk_flag == TRUE)
      {break}
    }
    
    mk <- c(0,mk,length(y))
    mk <- unique((sort(mk)))
    
    q <- mk
  }else{
    q <- c(0,q,length(q))
    q <- unique(sort(q))
  }
  
  #Anomaly check
  #Overall logic , here we remove the periodic median values from the series
  #We then identify the anomalies using MAD cutoff per identified level
  #Initalalizations
  y_med <- y
  temp1 <- y
  s <-q
  
  window_medians <- vector()
  outliers <- vector()
  outliers_new <- vector()
  anomalies <- vector()
  
  for( i in seq(from = 1, to = c(length(s)-1), by =1))
  {
    window_len <- 14
    
    win_len<- ceiling(length(y[c(s[i]+1):c(s[i+1])])/window_len)
    
    for(w in seq(1, (win_len)))
    {
      if(s[i]+window_len >= s[i+1])
      {
        window_len <- s[i+1] - s[i]
      }
      
      med_1 <- median(y_med[c(s[i]+1):c(s[i] +window_len)])
      y_med[c(s[i]+1):c(s[i] +window_len)] <- med_1
      
      #Storing medians
      window_medians  <- c(window_medians, rep(med_1, window_len))
      
      #updating length
      s[i] <- s[i]+window_len
    }
  }
  
  #Adjusting window median length
  win_len_new <- (length(window_medians)- length(y_med)) +1
  window_medians <- window_medians[c(win_len_new:length(window_medians))]
  
  #Subtracting the median values
  temp1 <- y - y_med
  
  
  #First pass , identify outliers based on deseaonlized values, that are not multiples of the frequency
  
  ##########################################################
  outliers_deseason <- vector()
  for(i in seq(from = 1, to = c(length(q)-1), by =1))
  {
    med_1 = mad(temp1[c(q[i]+1):c(q[i+1])], center = median(c(temp1[c(q[i]+1):c(q[i+1])])),constant = 1.4)
    median_val <- median(c(temp1[c(q[i]+1):c(q[i+1])]))
    
    for(p in seq(1, (length(temp1[c(q[i]+1):c(q[i+1])]))))
    {
      
      if(temp1[c(q[i]+1):c(q[i+1])][p] > c(median_val+(med_1*conf_level)) || temp1[c(q[i]+1):c(q[i+1])][p] < c(median_val-(med_1*conf_level)))
      {
        #Writing the anomalies out
        outliers_deseason <- c(outliers_deseason, c(p+q[i]))
      }
    }
  }
  
  #52 frequncey check
  dif_mat_new <- outer(outliers_deseason, outliers_deseason, '-')
  graph_new <- which(dif_mat_new == frequency, arr.ind= TRUE)
  graph_new <- data.frame(graph_new)
  
  pairs_1_new <- vector()
  pairs_new <- vector()
  
  if(dim(graph_new)[1] != 0)
  {
    for(t in seq(from =1, to = c(dim(graph_new)[1]), by = 1))
    {
      pairs_new <- c(outliers_deseason[graph_new['row'][t,]], outliers_deseason[graph_new['col'][t,]])
      pairs_1_new <- c(pairs_1_new, pairs_new)
    }
  }
  #No outliers that have 52 freq aka seasonal outliers
  outliers_deseason <- outliers_deseason[!(outliers_deseason %in% pairs_1_new)]
  
  ########################################################################
  anomalies <- sort(c(outliers_deseason))
  
  
  for( i in seq(from = 1, to = c(length(q)-1), by =1))
  {
    med_1 = mad(temp1[c(q[i]+1):c(q[i+1])], center = median(c(temp1[c(q[i]):c(q[i+1])])),constant = 1.3)
    median_val <- median(c(temp1[c(q[i]+1):c(q[i+1])]))
    
    #Extacting the points in the anomalies
    anom_new <- vector()
    anom_new <- anomalies[between(anomalies, q[i], q[i+1])]
    
    if(i ==1)
    {
      anom_new <- anom_new - c(q[i])
    }else{
      anom_new <- anom_new - c(q[i]-1)
      
    }
    
    for(p in seq(1, (length(anom_new))))
    {
      
      if(i == 1)
      {
        
        if(is.na(temp1[c(q[i]+1):c(q[i+1])][anom_new[p]]))
        {break}
        
        if(temp1[c(q[i]+1):c(q[i+1])][anom_new[p]] > 0)
        {
          temp1[c(q[i]+1):c(q[i+1])][anom_new[p]] <- median_val+(med_1*conf_level)
        }else{
          temp1[c(q[i]+1):c(q[i+1])][anom_new[p]] <- median_val-(med_1*conf_level)
        }
        
      }else{
        
        if(is.na(temp1[c(q[i]+1):c(q[i+1])][anom_new[p]]))
        {break}
        
        if(is.na(temp1[c(q[i]+1):c(q[i+1])][anom_new[p+1]]))
        {next}
        
        if(temp1[c(q[i]+1):c(q[i+1])][anom_new[p+1]] > 0)
        {
          temp1[c(q[i]+1):c(q[i+1])][anom_new[p+1]] <- median_val+(med_1*conf_level)
        }else{
          temp1[c(q[i]+1):c(q[i+1])][anom_new[p+1]] <- median_val-(med_1*conf_level)
        }
        
      }
    }
  }
  
  final_x <- (temp1  + window_medians)
  
  y <- final_x
  
  anomalies <- sort(unique(anomalies))
  
  
  #Smoothening the series
  #Taking starting and ending points of the series
  k <- vector()
  k <- q
  k <- sort(k)
  
  #Smoothening the series
  trend_line <- vector()
  if(length(k) == 0)
  {
    q = lowess(Data)
    trend_line = c(trend_line, q$y)
  }else
  {
    trend_line <- vector()
    pointer <- vector()
    
    for(i in seq(from = 0, to = c(length(k)-2), by = 1))
    {
      
      if(i == 0)
      {
        pointer <- 0
      }else{
        pointer <- 1
      }
      v <- y[(c(k[i+1]+pointer): k[i+2])]
      q = lowess(v)
      trend_line = c(trend_line, q$y)
    }
    
  }
  
  
  #Detrending the series
  de_trend <- c(Data-trend_line)
  
  
  #Transforming to a series
  ts.QTY1 = ts(data = as.vector(t(de_trend)), frequency = frequency)
  
  decomposed <- NA
  tryCatch(
    {
      decomposed <- stl(ts.QTY1, s.window = 'periodic')
    },error = function(e){ts_QTY1 <<- NULL}
  )
  
  if(length(decomposed) != 1)
  {
    seasonal <- decomposed$time.series[,1]
    trend <- decomposed$time.series[,2]
    remainder <- decomposed$time.series[,3]
    
    #Removing the seasonality
    ts_QTY1 <- ts.QTY1 - seasonal
  }else{
    
    seasonal <- NULL
    trend <- NULL
    remainder <- NULL
  }
  
  
  #Plotting function for anomalies
  if(plot)
  {
    #Plots the raw data as well as the shaded confidence intervals and anomalies as circles
    #Borrows from twitters visualization
    
    #Anomalies plot
    b <- Data
    
    #Cleaning data
    temp <- data.frame(b)
    
    colnames(temp) <- NULL
    rownames(temp) <- NULL
    
    temp$names <- rownames(temp)
    
    temp <- temp[,c(2,1)]
    #Getting time stamps as a single columb
    #Setting the row and column names
    
    if (any((names(temp) == c("timestamp", "count")) == FALSE)) {
      colnames(temp) <- c("timestamp", "count")
    }
    
    
    #Changing the numeric type
    temp$timestamp <- as.numeric(temp$timestamp)
    
    #Plotting based on anomalies
    anomalies_data <- temp[temp$timestamp %in% anomalies,]
    
    #Final Plot funtion
    visual <- ggplot(temp, aes(timestamp, count, group = 1), title = 'Anomalies') + geom_line(size = 0.5) +
      # ggplot2::geom_point(data = anomalies_data, color = "red") + labs( title = 'Anomalies', y = NULL)+
      theme(plot.title = element_text(hjust = 0.5, size = 14),
            plot.background=element_rect(fill = "white"),
            panel.background = element_rect(fill = 'white'),
            axis.line = element_line(colour = "grey"))
    plot(visual)
    return(plot = visual)
  }
}

#Setting unix Threading limits
library(unix)
rlimit_nproc(cur = Inf, max = NULL)
rlimit_stack(cur = Inf, max = NULL)
unix::rlimit_all()

#Trend calculation
Trend_cal=function(y)
{
  trend_data <- tryCatch({ds_series_new(y)$trend_line},error=function(e){
    first_val <- which(!y <= 0)
    trend_data = lowess(y[first_val[1]:length(y)])$y
    
    if(all(unique(trend_data) == 0))
    {
      trend_val <- mean(y[first_val[1]:length(y)])
      trend_data <- rep(trend_val, times = length(y[first_val[1]:length(y)]))
    }
    
    return(trend_data)
  })
  
  return(trend_data)
}

#Generalized correlation function
cor2 = function(df)
{
  
  stopifnot(inherits(df, "data.frame"))
  stopifnot(sapply(df, class) %in% c("integer"
                                     , "numeric"
                                     , "factor"
                                     , "character"))
  
  cor_fun <- function(pos_1, pos_2)
  {
    
    # both are numeric
    if(class(df[[pos_1]]) %in% c("integer", "numeric") &&
       class(df[[pos_2]]) %in% c("integer", "numeric")){
      r <- stats::cor(df[[pos_1]]
                      , df[[pos_2]]
                      , use = "everything"
      )
    }
    
    # one is numeric and other is a factor/character
    if(class(df[[pos_1]]) %in% c("integer", "numeric") &&
       class(df[[pos_2]]) %in% c("factor", "character")){
      r <- sqrt(
        summary(
          stats::lm(df[[pos_1]] ~ as.factor(df[[pos_2]])))[["r.squared"]])
    }
    
    if(class(df[[pos_2]]) %in% c("integer", "numeric") &&
       class(df[[pos_1]]) %in% c("factor", "character")){
      r <- sqrt(
        summary(
          stats::lm(df[[pos_2]] ~ as.factor(df[[pos_1]])))[["r.squared"]])
    }
    
    # both are factor/character
    if(class(df[[pos_1]]) %in% c("factor", "character") &&
       class(df[[pos_2]]) %in% c("factor", "character")){
      r <- lsr::cramersV(df[[pos_1]], df[[pos_2]], simulate.p.value = TRUE)
    }
    
    return(r)
  } 
  
  cor_fun <- Vectorize(cor_fun)
  
  # now compute corr matrix
  corrmat <- outer(1:ncol(df)
                   , 1:ncol(df)
                   , function(x, y) cor_fun(x, y)
  )
  
  rownames(corrmat) <- colnames(df)
  colnames(corrmat) <- colnames(df)
  
  return(corrmat)
}

#Data Cleaning 
Data_clean = function(Data_unclean, calendar)
{
  colnames(Data_unclean)<- toupper(colnames(Data_unclean))
  
  #Defining the region
  Data_unclean[, WM_REGION:='NATIONAL']
  
  #Removing factor in page location
  #Data_unclean$PAGE_LOCATION <- as.character(Data_unclean$PAGE_LOCATION)
  
  #Setting the null values
  Data_unclean <- Data_unclean[calendar, on ='WM_YR_WK']
  Data_unclean[is.na(Data_unclean)] <- 0
  
  Data_unclean$GROUP_CODE  = sort(unique(Data_unclean$GROUP_CODE),  decreasing = TRUE)[1]
  Data_unclean$DEPT_NBR  = sort(unique(Data_unclean$DEPT_NBR),  decreasing = TRUE)[1]
  Data_unclean$WM_REGION  = sort(unique(Data_unclean$WM_REGION), decreasing = TRUE)[1]
  
  #Note: Convert all to data to table in future for speed, for now this is fine
  #Data_unclean <- data.frame(Data_unclean)
  
  #Setting a rolling mode value to handle the zero REG/LIVE prices
  Data_unclean$LIVE_PRICE <- Rolled(data = Data_unclean[,LIVE_PRICE], mode_length = 4, pad_number = 4)
  Data_unclean$REG_PRICE <- Rolled(data = Data_unclean[,REG_PRICE], mode_length = 4, pad_number = 4)
  
  Data_unclean$REG_PRICE <- as.numeric(Data_unclean$REG_PRICE)
  Data_unclean$LIVE_PRICE <- as.numeric(Data_unclean$LIVE_PRICE)
  
  #Cleaning the Negative QTY_COUNT, Sales and QTY
  if("TOTAL_TRANSACTION_COUNT" %in% colnames(Data_unclean))
  {
    Data_unclean$TOTAL_TRANSACTION_COUNT[Data_unclean$TOTAL_TRANSACTION_COUNT < 0] <- 0
  }else
  {
    Data_unclean$MODELLED_QTY[Data_unclean$MODELLED_QTY < 0] <- 0
  }
  
  #Setting the new flyer flags
  if("BACK" %in% Data_unclean[,PAGE_LOCATION] || "INSIDE" %in% Data_unclean[,PAGE_LOCATION] || "FRONT" %in% Data_unclean[,PAGE_LOCATION])
  {
    dmy <- dummyVars(" ~ PAGE_LOCATION", data = Data_unclean, fullRank = T)
    flyer_new <- data.frame(predict(dmy, newdata = Data_unclean))
    Missing_cols <- setdiff(c('PAGE_LOCATIONFRONT', 'PAGE_LOCATIONBACK', 'PAGE_LOCATIONINSIDE'), c(colnames(flyer_new)))
    if(length(Missing_cols) > 0)
    {
      flyer_new <- cbind(flyer_new, setNames(lapply(Missing_cols, function(x) x=0), Missing_cols) )
    }
    
  }else
  {
    Missing_cols = c('PAGE_LOCATIONFRONT', 'PAGE_LOCATIONBACK', 'PAGE_LOCATIONINSIDE')
    flyer_new = data.frame(matrix(ncol = 3, nrow = week_cnt))
    flyer_new[is.na(flyer_new)] <- 0
    colnames(flyer_new) <- Missing_cols    
  }
  
  Data_unclean$SALES[Data_unclean$SALES < 0] <- 0
  
  #Binding with the og data
  Data_unclean <- cbind(Data_unclean, flyer_new)
  
  #Removing the base flyer base location
  Data_unclean[,PAGE_LOCATION :=NULL]
  
  return(Data_unclean)
  
}

#Mode function
Mode <- function(x, na.rm = TRUE) 
{
  if(na.rm){
    x = x[!is.na(x)]
  }
  ux <- unique(x) 
  if((ux == 0) && (length(ux) ==1))
  {
    return(ux[which.max(tabulate(match(x, ux)))])
  }else{ux <- ux[ux !=0]
  return(ux[which.max(tabulate(match(x, ux)))])
  }
}

#Padding function
Padded <- function(data, pad_number)
{
  
  #Top padding 
  v2 <- c(1:c(length(data)-pad_number))
  data <- c(rep(NA, length(data) - length(v2)), data)
  
  #Back padding 
  v2 <- c(1:c(length(data)+pad_number))
  length(data) <- length(v2)
  
  return(data)
}

#Rolling Mode
Rolled <- function(data, mode_length, pad_number)
{
  #Padding
  data <- Padded(data, pad_number)
  
  #Rolling 
  for(i in 1:(length(data) - mode_length + 1))
  {
    if((is.na(data[i])) || (data[i]!=0))
    {next}else{
      data[i] <- Mode(data[c(c(i-mode_length):c(i+mode_length))])
    }
    
  }
  data <- data[!is.na(data)]
  return(data)
}

#Main function
Data_prep = function(xd)
{
  GC = GroupCode[xd]
  
  Data_final <- Base_dept_data[J(GC) ,nomatch = 0L]
  
  #Cleaning GC data
  Data_final$MODELLED_QTY <- Data_final$QTY
  Data_final = Data_clean(Data_final, calendar = calendar)
  
  rownames(Data_final) <- NULL
  
  #Lag Addition, 4 lags of the promo variables
  Promotions_df <- Data_final[,..Promotions]
  lag_1 = as.data.table(shift(Promotions_df, n=1L, fill=NA, type=c("lag"), give.names=TRUE))
  lag_2 = as.data.table(shift(Promotions_df, n=2L, fill=NA, type=c("lag"), give.names=TRUE))
  lag_3 = as.data.table(shift(Promotions_df, n=3L, fill=NA, type=c("lag"), give.names=TRUE))
  lag_4 = as.data.table(shift(Promotions_df, n=4L, fill=NA, type=c("lag"), give.names=TRUE))
  
  #Appending new lagged cols
  Data_final <- cbind(Data_final,lag_1,lag_2,lag_3,lag_4)
  
  #Set lag values to 0 whenever theres a promotion
  # Data_final <- within(Data_final, {
  #   f <- PA_WEIGHTED_REG != 0 | AR_WEIGHTED_REG != 0 | TR_WEIGHTED_REG != 0 | DIGEST_FLAG == 1 | DIGITAL_FLAG == 1 | CPP_FLAG == 1 | PAGE_LOCATIONINSIDE == 1 | PAGE_LOCATIONFRONT == 1 | PAGE_LOCATIONBACK == 1
  #   PA_WEIGHTED_REG_lag_1[f] <- 0
  #   AR_WEIGHTED_REG_lag_1[f] <- 0
  #   TR_WEIGHTED_REG_lag_1[f] <- 0
  #   DIGEST_FLAG_lag_1[f] <- 0
  #   DIGITAL_FLAG_lag_1[f] <- 0
  #   CPP_FLAG_lag_1[f] <- 0
  #   PAGE_LOCATIONINSIDE_lag_1[f] <- 0
  #   PAGE_LOCATIONFRONT_lag_1[f] <- 0
  #   PAGE_LOCATIONBACK_lag_1[f] <- 0
  #   
  #   PA_WEIGHTED_REG_lag_2[f] <- 0
  #   AR_WEIGHTED_REG_lag_2[f] <- 0
  #   TR_WEIGHTED_REG_lag_2[f] <- 0
  #   DIGEST_FLAG_lag_2[f] <- 0
  #   DIGITAL_FLAG_lag_2[f] <- 0
  #   CPP_FLAG_lag_2[f] <- 0
  #   PAGE_LOCATIONINSIDE_lag_2[f] <- 0
  #   PAGE_LOCATIONFRONT_lag_2[f] <- 0
  #   PAGE_LOCATIONBACK_lag_2[f] <- 0
  #   
  #   PA_WEIGHTED_REG_lag_3[f] <- 0
  #   AR_WEIGHTED_REG_lag_3[f] <- 0
  #   TR_WEIGHTED_REG_lag_3[f] <- 0
  #   DIGEST_FLAG_lag_3[f] <- 0
  #   DIGITAL_FLAG_lag_3[f] <- 0
  #   CPP_FLAG_lag_3[f] <- 0
  #   PAGE_LOCATIONINSIDE_lag_3[f] <- 0
  #   PAGE_LOCATIONFRONT_lag_3[f] <- 0
  #   PAGE_LOCATIONBACK_lag_3[f] <- 0
  #   
  #   PA_WEIGHTED_REG_lag_4[f] <- 0
  #   AR_WEIGHTED_REG_lag_4[f] <- 0
  #   TR_WEIGHTED_REG_lag_4[f] <- 0
  #   DIGEST_FLAG_lag_4[f] <- 0
  #   DIGITAL_FLAG_lag_4[f] <- 0
  #   CPP_FLAG_lag_4[f] <- 0
  #   PAGE_LOCATIONINSIDE_lag_4[f] <- 0
  #   PAGE_LOCATIONFRONT_lag_4[f] <- 0
  #   PAGE_LOCATIONBACK_lag_4[f] <- 0
  #   
  # })
  #Data_final <- subset(Data_final, select = -c(f))
  
  #Removing top 4 rows 
  Data_final <- Data_final[-(1:4),]
  
  #Adding seasonality decomposition
  if(nrow(Data_final) >= 104) #We can change this to simple sparsity and seasonality testing 
  {
    full_periods = floor(nrow(Data_final)/52)
    ses_var = as.factor(rep(c(1:52),full_periods))
    
    if(nrow(Data_final) %% 52 != 0)
    {
      p = nrow(Data_final) - (52*full_periods)
      ses_var = as.factor(c(ses_var, c(1:p)))
    }
    
    temp_season = data.frame(ses_var)
    colnames(temp_season) = 'week'
    ts = dummyVars(~.,data = temp_season)
    temp_season = data.frame(predict(ts,newdata = temp_season))
    temp_season[colnames(temp_season)] = lapply(temp_season[colnames(temp_season)],factor)
    
    Data_final = cbind(Data_final, temp_season)
  }
  
  #Addition of trend, only on the training data
  Trend_var <- tryCatch({Trend_cal(Data_final[,MODELLED_QTY])}, error = function(e){Trend_var <- c(rep(median(Data_final[,MODELLED_QTY])), c(nrow(Data_final)))})
  
  #Padding leading zeros with zero trend, if dropped
  if(nrow(Data_final) - length(Trend_var) != 0)
  {
    pad <- c(nrow(Data_final) - length(Trend_var))
    Trend_var <- c(rep(0, pad), Trend_var)
  }
  
  Data_final[,Trend_var := Trend_var]
  
  #Anomaly Detection
  Data_final <- Anomaly_detection(Data_final)
  
  #Final cleaning
  Data_final[is.na(Data_final)] <- 0
  
  system(paste("echo Function call", GC))
  
  return (Data_final)
}

#Anomaly Detection
Anomaly_detection = function(data_orig)
{
  conf_level <- 2
  outliers <- vector()
  promo_points <- vector()
  
  #Detrending
  ts_MODELLED_QTY <- data_orig[,MODELLED_QTY] - data_orig[,Trend_var]
  
  #Removing seasonality wks if it exists
  if(nrow(data_orig) >= 104)
  {
    lm.ses <- lm(ts_MODELLED_QTY~.,data = data_orig[,grep('week', names(data_orig)), with = FALSE])
    ts_MODELLED_QTY <- ts_MODELLED_QTY - lm.ses$fitted.values
  }
  
  #Defining the weeks where even if promo is present, we consider anomalies
  Anomalous_promo <- c(202005, 202006, 202007, 202008, 202009, 202010, 202011, 202012, 202013)
  Anomalous_points <- which(data_orig$WM_YR_WK %in% Anomalous_promo)
  
  #Not considering the points that have promotions 
  promo_points <-rowSums(data_orig[,c(Promotions), with = FALSE])
  promo_points <- which(promo_points!=0 )
  
  #Removing anomalies , overall , no need per level, dropping all points that are zero 
  if(length(promo_points)!=0)
  {
    median_val <- median(ts_MODELLED_QTY[-promo_points])
    
    if(is.na(median_val))
    {
      return(data_orig)
    }
    
    med_1 = mad((ts_MODELLED_QTY[-promo_points]), center = median(ts_MODELLED_QTY[-promo_points]),constant = 1.5)
  }else
  {
    median_val <- median(ts_MODELLED_QTY)
    med_1 <- mad((ts_MODELLED_QTY), center = median(ts_MODELLED_QTY), constant = 1.5)
  }
  
  #Rownumber corresponding to them
  Anomalous_promo_rownames <- rownames(data_orig[data_orig[,WM_YR_WK] %in%  Anomalous_promo,])
  
  #Weeks that we consider outliers
  Anomalous_weeks <- data_orig$WM_YR_WK[(which(data_orig$WM_YR_WK >= 202007 & data_orig$WM_YR_WK <= 202020))]
  
  #Storing all anomalies in the series 
  for(h in seq(from = 1, to = c(length(ts_MODELLED_QTY)-1), by =1))
  {
    if(ts_MODELLED_QTY[h] > c(median_val + (med_1*conf_level)))
    {
      
      if(!(h %in% c(promo_points[!(promo_points %in% Anomalous_promo_rownames)])))
      { 
        #Storing anomalies 
        outliers <- c(outliers, c(h))
      }
    }
    
    if(ts_MODELLED_QTY[h] < c(median_val-(med_1*conf_level)))
    {
      if(!(h %in% c(promo_points[!(promo_points %in% Anomalous_promo_rownames)])))
      { 
        outliers <- c(outliers, c(h))
      }
    }
  }
  
  outlier_covid = Anomalous_points[which(Anomalous_points %in% outliers)]
  if(length(outlier_covid) == 0)
  {
    return(data_orig)
  }
  
  for(i in 1:length(outlier_covid))
  {
    data_orig$MODELLED_QTY[outlier_covid[i]] = median_val + data_orig[,Trend_var][outlier_covid[i]] + lm.ses$fitted.values[outlier_covid[i]]
  }
  
  return(data_orig)
}

#Correlation Function
Pf_func =  function(List_Frame)
{
  
  lm_dat <- Relevant_pf_perDept[[List_Frame]]
  
  #if(unique(lm_dat$MODELLED_QTY) == 0)
  if(all(lm_dat$MODELLED_QTY==0))
  {return(data.table())}
  
  #Detrending the series 
  lm_dat$MODELLED_QTY <- c(lm_dat$MODELLED_QTY - lm_dat$Trend_var)
  lm_dat$MODELLED_QTY[lm_dat$MODELLED_QTY < 0] <- 0
  
  Weeks <- names(lm_dat)[grep("week", names(lm_dat))]
  Holiday_vars <- c(names(lm_dat)[grep("_DAY",names(lm_dat))], 'GOOD_FRIDAY', 'NEW_YEARS', 'ANNIVERSARY_OF_THE_STATUE_OF_WESTMINISTER')
  Focus_Promos <- names(lm_dat)[grep("FOCUS",names(lm_dat))]
  Lag_var <- colnames(lm_dat)[grep("_lag_",colnames(lm_dat))]
  
  
  #Running a linear model removing the fitted values
  temp_lm_dat <- as.data.frame(cbind(lm_dat[, ..Promotions], lm_dat[, ..Weeks],   
                                     lm_dat[, ..Holiday_vars],
                                     'MODELLED_QTY' = lm_dat[,MODELLED_QTY]))
  
  temp_lm_dat <- as.data.table(sapply(temp_lm_dat, as.character))
  temp_lm_dat <- as.data.table(sapply(temp_lm_dat, as.numeric))
  temp_lm_dat <- temp_lm_dat[ , lapply(.SD, function(v) if(uniqueN(v) > 1) v)]
  
  if(!("MODELLED_QTY" %in% colnames(temp_lm_dat))){return(data.table())}
  
  lm.ses = lm(MODELLED_QTY ~., data = temp_lm_dat)
  Pf_QTY = c(lm_dat$MODELLED_QTY - lm.ses$fitted.values)
  
  if(unique(Pf_QTY) == 0)
  {return(data.table())}
  
  Final_pf <- cbind(data.table(Pf_QTY), lm_dat[, ..Lag_var])
  Final_pf <- data.table(Final_pf)
  colnames(Final_pf)[1] <- c('QTY') 
  Final_pf <- Final_pf[ , lapply(.SD, function(v) if(uniqueN(v) > 1) v)]
  
  if(dim(Final_pf)[1] == 0 | dim(Final_pf)[2] == 1)
  {return(data.table())}
  
  Final_pf <- as.data.table(sapply(Final_pf, as.character))
  Final_pf <- as.data.table(sapply(Final_pf, as.numeric))
  
  #Correlation checking
  Cor_values_pf <- data.frame(cor(Final_pf))
  if(dim(Cor_values_pf)[2] == 1){return(data.table())}
  
  Relevant_pf_info <- list()
  for(cor_iter in 1:(length(Cor_values)))
  {
    Pf_promotions <- colnames(Cor_values_pf[colSums(Cor_values_pf[1,]) <= Cor_values[cor_iter]])
    Pf_promotions <- Pf_promotions[Pf_promotions %in% Lag_var]
    Pf_promotions <-  paste(shQuote(Pf_promotions, type="sh"), collapse=", ")
    
    if(Pf_promotions != "")
    { 
      
      #All relevent info added to a single table
      Relevant_pf_info_inital <- data.frame()
      Relevant_pf_info_inital <- data.frame(cbind( unique(lm_dat$GROUP_CODE), Cor_values[cor_iter], Pf_promotions, Dept_Nbr))
      colnames(Relevant_pf_info_inital) <- c('FOCUS_GC', 'CORRELATION_LEVEL', 'PF_PROMOTIONS', 'FGC_DEPT_NBR') 
      
    }else{break}
    
    #Adding this to a bigger frame
    Relevant_pf_info[[cor_iter]] <- Relevant_pf_info_inital
    
  }
  
  #Combine all into one
  Relevant_pf_info <- rbindlist(Relevant_pf_info)
  
  system(paste("echo Function call", List_Frame))
  
  
  return(Relevant_pf_info)
  
}



#Dept list
#Depts <- paste("SELECT DISTINCT DEPT_NBR FROM ca_pricing_data_science_tables.promox_model_data_national_rollup_mrktng_flyer WHERE DEPT_NBR <> 38 AND DEPT_NBR <> 49 AND DEPT_NBR <> 50 ORDER BY DEPT_NBR")
#Depts <- dataset.load(name = 'GCP DD', query = Depts)
#colnames(Depts) <- toupper(colnames(Depts))
#Depts <- Depts$DEPT_NBR
Depts <- c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,38,40,46,72,80,81,82,83,85,87,90,91,92,93,94,95,96,97,98)



#Calendar Data
rolling_weeks <- 203
Forecast_length <- 12
#week_end <- 202345
#week_end <- getArgument("week_end","PARAMS")
#week_end <- as.integer(week_end)

week_end <- paste("select max(wm_yr_wk) from dbcure.promox_model_data_national_rollup_mrktng_flyer")
#week_start <- dataset.load(name = 'GCP DD', query = week_start)
week_end <- contry ('load',10,5,week_end)
week_end <- as.numeric(week_end)

week_start <- paste("select min(wm_yr_wk) from (select distinct wm_yr_wk from dbcure.promox_marketing_wm_calendar where wm_yr_wk<",week_end," order by wm_yr_wk desc limit ",rolling_weeks,") a")
#week_start <- dataset.load(name = 'GCP DD', query = week_start)
week_start <- contry ('load',10,5,week_start)
week_start <- as.numeric(week_start)

week_forecast_end <- paste("SELECT MAX(WM_YR_WK) FROM (SELECT DISTINCT WM_YR_WK FROM dbcure.promox_marketing_wm_calendar WHERE WM_YR_WK > ",week_end," ORDER BY WM_YR_WK ASC LIMIT ",Forecast_length,") a")
#week_forecast_end <- dataset.load(name = 'GCP DD', query = week_forecast_end)
week_forecast_end <- contry('load',10,5,week_forecast_end)
week_forecast_end <- as.numeric(week_forecast_end)

week_forecast_start <- paste("SELECT MAX(WM_YR_WK) FROM (SELECT DISTINCT WM_YR_WK FROM dbcure.promox_marketing_wm_calendar WHERE WM_YR_WK > ",week_end," ORDER BY WM_YR_WK ASC LIMIT 1) a")
#week_forecast_start <- dataset.load(name = 'GCP DD', query = week_forecast_start)
week_forecast_start <- contry('load',10,5,week_forecast_start)
week_forecast_start <- as.numeric(week_forecast_start)

calendar <- paste("SELECT distinct wm_yr_wk, start_date,end_date FROM dbcure.promox_marketing_wm_calendar WHERE WM_YR_WK BETWEEN ",week_start," AND ",week_end," ORDER BY WM_YR_WK")
#calendar <- dataset.load(name = 'GCP DD', query = calendar)
calendar <- contry('load',10,5,calendar)
names(calendar) <- toupper(names(calendar))

week_cnt <- dim(calendar)[1]

#List of Promotions
Promotions <- c('PA_WEIGHTED_REG', 'AR_WEIGHTED_REG', 'TR_WEIGHTED_REG', 
                'PAGE_LOCATIONBACK', 'PAGE_LOCATIONFRONT', 'PAGE_LOCATIONINSIDE',
                'CPP_FLAG', 'DIGITAL_FLAG', 'DIGEST_FLAG')

Promos <- c('PA_WEIGHTED_REG', 'AR_WEIGHTED_REG', 'TR_WEIGHTED_REG', 
            'FLYER_FLAG','CPP_FLAG', 'DIGITAL_FLAG', 'DIGEST_FLAG')

#Correlation values to run through
Cor_values <- c(-0.000001)

#Extracting Base information
query <- paste("SELECT * FROM dbcure.promox_model_data_national_rollup_mrktng_flyer WHERE WM_YR_WK BETWEEN", week_start," AND ", week_end, " AND DEPT_NBR IN (",paste(Depts,collapse=", "),") ORDER BY GROUP_CODE, WM_YR_WK")
#Base_data <- dataset.load(name = 'GCP DD', query = query)
Base_data <- contry('load',10,5,query)
Base_data <- as.data.table(Base_data)

#Removing the table name from the base data
names(Base_data) = gsub(pattern = "*promox_model_data_national_rollup_mrktng_flyer.", replacement = "", x = names(Base_data))
names(Base_data) <- toupper(names(Base_data))
Base_data$GROUP_CODE = as.character(Base_data$GROUP_CODE)
Base_data$WM_YR_WK = as.numeric(Base_data$WM_YR_WK)
Base_data$DEPT_NBR = as.numeric(Base_data$DEPT_NBR)
setkey(Base_data, DEPT_NBR, GROUP_CODE)

#GC keyed level table
Base_data_GC <- copy(Base_data)
setkey(Base_data_GC, GROUP_CODE)

#Forecast data

#GCs on promo from forecast table
query <- paste("SELECT DISTINCT GROUP_CODE, DEPT_NBR FROM dbcure.PROMOX_MODEL_FORECAST_DATA WHERE FLYER_FLAG = 1 AND WM_YR_WK BETWEEN ",week_forecast_start," AND ",week_forecast_end," AND DEPT_NBR IN (",paste(Depts,collapse=", "),")")
#Base_forecast_GC <- dataset.load(name = 'GCP DD', query = query)
Base_forecast_GC <- contry('load',10,5,query)
Base_forecast_GC <- as.data.table(Base_forecast_GC)

names(Base_forecast_GC) <- toupper(names(Base_forecast_GC))
Base_forecast_GC$GROUP_CODE = as.character(Base_forecast_GC$GROUP_CODE)
Base_forecast_GC$DEPT_NBR = as.numeric(Base_forecast_GC$DEPT_NBR)
setkey(Base_forecast_GC, DEPT_NBR)


Time_dp_s = Sys.time()

for(x in 1:length(Depts))
{
  Dept_Nbr = Depts[x]
  
  ##Data Load 
  
  ##Base Dept Data
  Base_dept_data <- Base_data[J(Dept_Nbr) ,nomatch = 0L]
  setkey(Base_dept_data, GROUP_CODE, WM_YR_WK)
  
  #Extracting the list of group codes that are on promotion in the week
  Focus_GC_list <- unique(Base_dept_data[rowSums(Base_dept_data[,c('FLYER_FLAG','CPP_FLAG','DIGEST_FLAG','DIGITAL_FLAG')]) > 0 & WM_YR_WK == week_end]$GROUP_CODE)
  
  ##Base Forecast Data
  Base_forecast_dept_GC <- Base_forecast_GC[J(Dept_Nbr) ,nomatch = 0L]
  setkey(Base_forecast_dept_GC, GROUP_CODE)
  
  #Extracting the list of FC group codes that are on promotion in 12 weeks
  FC_Focus_GC_list <- unique(Base_forecast_dept_GC$GROUP_CODE)
  
  ##End of Data Load
  
  ##Data Initalization
  
  #Unioning all GroupCodes
  GroupCode <- unique(c(Focus_GC_list, FC_Focus_GC_list))
  GroupCode = sort(GroupCode)
  
  if(length(GroupCode)==0)
  {
    next
  }
  
  data_not_generated = numeric()
  final_insights_allgc = data.frame()  
  
  #Modelling
  out_batchsize = 30
  for(jj in seq(1, length(GroupCode), out_batchsize))
  {
    if(c(length(GroupCode) - jj) < out_batchsize)
    {
      
      loop_again = TRUE
      
      while(loop_again)
      {
        loop_again = FALSE
        gc()
        
        tryCatch(
          {
            R.utils::withTimeout({
              Relevant_pf_perDept <- mclapply(c(jj:length(GroupCode)),  Data_prep, mc.cores = 18, mc.cleanup = TRUE)
              gc()
              
            }, timeout = 300, elapsed = 300, cpu = Inf)
          }, TimeoutException = function(ex){loop_again <<- TRUE}
        )
      }
      
      Relevant_pf_perDept <- Filter(function(k) length(k) > 0, Relevant_pf_perDept)
      
      if(length(Relevant_pf_perDept)==0){next}
      gc()
      
      batchsize <- 300
      for(j in seq(1,length(Relevant_pf_perDept), batchsize))
      {
        loop_again = TRUE
        Relevant_pf_perDept_res <- vector(mode = "list", length = batchsize)
        
        while(loop_again)
        {
          loop_again = FALSE
          
          tryCatch(
            {
              R.utils::withTimeout({
                if(length(Relevant_pf_perDept) - j < batchsize)
                {
                  Relevant_pf_perDept_res <- mclapply(c(j:length(Relevant_pf_perDept)),  Pf_func, mc.cores = 18, mc.cleanup = TRUE)
                  Final_df <- rbindlist(Relevant_pf_perDept_res)
                  
                  #Writing to Elements local
                  colnames(Final_df) <- gsub(x = colnames(Final_df), pattern = "\\.", replacement = "_")
                  res_strng <- paste("/data/GCP/PF_Corr/PF_auto/", as.character(Dept_Nbr), "_xpf_GC_","_",jj,"_",j, "_", as.character(Sys.Date()), ".csv", sep="")
                  #res_strng_backup <- paste("/user/svccaiaproddse/Weekly_Scorecard_backup/PullForward/PullForward_auto/", as.character(Dept_Nbr), "_xpf_GC_","_",jj,"_",j, "_", as.character(Sys.Date()), ".csv", sep="")
                  
                  if(dim(Final_df)[1] > 0)
                  {
                    Final_df$SCORECARD_WK <- week_end
                    write.csv(Final_df,paste0(res_strng),row.names = FALSE)
                  }
                  
                  
                  gc()
                  
                  break
                }
                
                Relevant_pf_perDept_res <- mclapply(c(j:c(j + batchsize - 1)),  Pf_func, mc.cores = 18, mc.cleanup = TRUE)
                Final_df <- rbindlist(Relevant_pf_perDept_res)
                
                #Writing to Elements local
                colnames(Final_df) <- gsub(x = colnames(Final_df), pattern = "\\.", replacement = "_")
                res_strng <- paste("/data/GCP/PF_Corr/PF_auto/", as.character(Dept_Nbr), "_xpf_GC_","_",jj,"_",j, "_", as.character(Sys.Date()), ".csv", sep="")
                #res_strng_backup <- paste("/user/svccaiaproddse/Weekly_Scorecard_backup/PullForward/PullForward_auto/", as.character(Dept_Nbr), "_xpf_GC_","_",jj,"_",j, "_", as.character(Sys.Date()), ".csv", sep="")
                
                if(dim(Final_df)[1] > 0)
                {
                  Final_df$SCORECARD_WK <- week_end
                  write.csv(Final_df,paste0(res_strng),row.names = FALSE)
                }
                
                
                gc()
                
              }, timeout = 800, elapsed = 800, cpu =Inf)
            }, TimeoutException = function(ex){loop_again <<- TRUE}
          )
        }    
      }
      
    }else
    {
      
      loop_again = TRUE
      
      while(loop_again)
      {
        loop_again = FALSE
        gc()
        
        tryCatch(
          {
            R.utils::withTimeout({
              Relevant_pf_perDept <- mclapply(c(jj : (jj + out_batchsize - 1)), Data_prep, mc.cores = 18, mc.cleanup = TRUE)
              gc()
              
            }, timeout = 300, elapsed = 300, cpu = Inf)
          }, TimeoutException = function(ex){loop_again <<- TRUE}
        )
      }
      
      Relevant_pf_perDept <- Filter(function(k) length(k) > 0, Relevant_pf_perDept)
      
      if(length(Relevant_pf_perDept) == 0){next}
      gc()
      
      batchsize <- 300
      for(j in seq(1, length(Relevant_pf_perDept), batchsize))
      {
        loop_again = TRUE
        Relevant_pf_perDept_res <- vector(mode = "list", length = batchsize)
        
        while(loop_again)
        {
          loop_again = FALSE
          
          tryCatch(
            {
              R.utils::withTimeout({
                if(length(Relevant_pf_perDept) - j < batchsize)
                {
                  Relevant_pf_perDept_res <- mclapply(c(j:length(Relevant_pf_perDept)),  Pf_func, mc.cores = 18, mc.cleanup = TRUE)
                  Final_df <- rbindlist(Relevant_pf_perDept_res)
                  
                  #Writing to Elements local
                  colnames(Final_df) <- gsub(x = colnames(Final_df), pattern = "\\.", replacement = "_")
                  res_strng <- paste("/data/GCP/PF_Corr/PF_auto/", as.character(Dept_Nbr), "_xpf_GC_","_",jj,"_",j, "_", as.character(Sys.Date()), ".csv", sep="")
                  #res_strng_backup <- paste("/user/svccaiaproddse/Weekly_Scorecard_backup/PullForward/PullForward_auto/", as.character(Dept_Nbr), "_xpf_GC_","_",jj,"_",j, "_", as.character(Sys.Date()), ".csv", sep="")
                  
                  if(dim(Final_df)[1] > 0)
                  {
                    Final_df$SCORECARD_WK <- week_end
                    write.csv(Final_df,paste0(res_strng),row.names = FALSE)
                  }
                  
                  
                  gc()
                  
                  break
                }
                
                Relevant_pf_perDept_res <- mclapply(c(j:c(j + batchsize - 1)),  Pf_func, mc.cores = 18, mc.cleanup = TRUE)
                Final_df <- rbindlist(Relevant_pf_perDept_res)
                
                #Writing to Elements local
                colnames(Final_df) <- gsub(x = colnames(Final_df), pattern = "\\.", replacement = "_")
                res_strng <- paste("/data/GCP/PF_Corr/PF_auto/", as.character(Dept_Nbr), "_xpf_GC_","_",jj,"_",j, "_", as.character(Sys.Date()), ".csv", sep="")
                #res_strng_backup <- paste("/user/svccaiaproddse/Weekly_Scorecard_backup/PullForward/PullForward_auto/", as.character(Dept_Nbr), "_xpf_GC_","_",jj,"_",j, "_", as.character(Sys.Date()), ".csv", sep="")
                
                if(dim(Final_df)[1] > 0)
                {
                  Final_df$SCORECARD_WK <- week_end
                  write.csv(Final_df,paste0(res_strng),row.names = FALSE)
                }
                
                gc()
                
              }, timeout = 800, elapsed = 800, cpu =Inf)
            }, TimeoutException = function(ex){loop_again <<- TRUE}
          )
        }    
      }
      
    }
  }
  
}

connector.upload(name="elements_to_gcs_prod", container="b76c14154ee4d5d351c531a47570f3a14ab1de74", source_path="/data/GCP/PF_Corr/PF_auto/", target_path="dbcure/PF_Corr/PF_auto")

Time_dp_e = Sys.time()

Time_dp = Time_dp_e - Time_dp_s
Time_tot = Time_dp_e - Time_tot_s



#38min

#Drop and create Hive table with HDFS data
#connector.execute(name='GCP DD', statement="drop table ca_pricing_data_science_tables.PROMOX_PF_CORR_SCORECARD purge")
#connector.execute(name='GCP DD', statement="create external table ca_pricing_data_science_tables.PROMOX_PF_CORR_SCORECARD (`FOCUS_GC` string, `CORRELATION_LEVEL` float, `PF_PROMOTIONS` string, `FGC_DEPT_NBR` int, `SCORECARD_WK` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS TEXTFILE LOCATION '/user/hive/warehouse/dbcure.db/Weekly_Scorecard/PullForward/PullForward_auto/' tblproperties ('skip.header.line.count'='1');")
