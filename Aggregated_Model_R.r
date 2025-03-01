rm(list=ls())
#.rs.restartR()
gc()

#Time_tot_s = Sys.time()

#Automatically detach all packages R
#invisible(lapply(paste0('package:', names(sessionInfo()$otherPkgs)), detach, character.only=TRUE, unload=TRUE))

#week_end <- getArgument("week_end","PARAMS")
#week_end <- as.integer(week_end)



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



f1 <- list.files('/data/GCP/Aggregated_Model/Results/', recursive = TRUE, full.names = TRUE, include.dirs = FALSE)
f2 <- list.files('/data/GCP/Aggregated_Model/Logs/', recursive = TRUE, full.names = TRUE, include.dirs = FALSE)
f3 <- list.files('/data/GCP/Aggregated_Model/FC/', recursive = TRUE, full.names = TRUE, include.dirs = FALSE)
f4 <- list.files('/data/GCP/Aggregated_Model/Halo/', recursive = TRUE, full.names = TRUE, include.dirs = FALSE)
f5 <- list.files('/data/GCP/Aggregated_Model/Cann/', recursive = TRUE, full.names = TRUE, include.dirs = FALSE)
f6 <- list.files('/data/GCP/Aggregated_Model/Insights/', recursive = TRUE, full.names = TRUE, include.dirs = FALSE)


file.remove(f1)
file.remove(f2)
file.remove(f3)
file.remove(f4)
file.remove(f5)
file.remove(f6)



rm(f1, f2, f3, f4,f5,f6)


#Data Preperation Libraries
#library(MultiLevelSTL)#Note:We need to change this to the latest version(Check how to change ver nbr in R packages)
library(mlutils)
library(strucchange)
library(Kendall)
library(purrr)
library(caret)
library(data.table)#Baseline function
setDTthreads(threads = 1)

#Modelling Libraries
library(doParallel)#Parallelization
library(foreach)#Parallelization
library(igraph)#Baseline function
library(Matrix)#Baseline function
library(tidyr)#Called only once , maybe datatable implementation/ If this is gone much simpler
library(dplyr)#used
library(car)#VIF calculation/Baseline function
library(ranger)#RF fit/Baseline function

#Novel method to decompose a level shifted time series 
#Library import
library(tseries)
library(digest)
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
  
  
  
  newList <- list('anomalies' = anomalies, 'adjusted_series' = y, 'trend_line' = trend_line, 'ds_series' = ts_QTY1, 'Seasonality' = seasonal, 'Trend' = trend,
                  'residual'= remainder, 'breakpoints' = k)
  return(newList)
  
}

library(unix)
unix::rlimit_all()
rlimit_nproc(cur = Inf, max = NULL)
rlimit_stack(cur = Inf, max = NULL)

packages_req=c("doParallel","foreach","MultiLevelSTL","DBI","car",
               "randomForest","changepoint","forecast","lsr","purrr",
               "tidyr","ellipsis","dplyr","stringr","data.table")

#Fetching global variables from workflow
#var_week <- getArgument("var_week","PARAMS")
#var_week <- as.integer(var_week)

#Base data prep function
Data_prep=function(xd)
{
  GC = GroupCode[xd]
  
  Data_final <- Base_dept_data[J(GC) ,nomatch = 0L]
  
  #Cleaning GC data
  Data_final$MODELLED_QTY <- Data_final$QTY
  Data_final = Data_clean(Data_final, calendar = calendar)
  
  rownames(Data_final) <- NULL
  
  #Cannibalization
  Data_final <- Cann_dataprep(GC, Data_final)
  
  #Halo
  Data_final <- Halo_dataprep(GC, Data_final)
  
  #Lag Addition, 4 lags of the promo variables
  Promotions_df <- Data_final[,..Promotions]
  lag_1 = as.data.table(shift(Promotions_df, n=1L, fill=NA, type=c("lag"), give.names=TRUE))
  lag_2 = as.data.table(shift(Promotions_df, n=2L, fill=NA, type=c("lag"), give.names=TRUE))
  lag_3 = as.data.table(shift(Promotions_df, n=3L, fill=NA, type=c("lag"), give.names=TRUE))
  lag_4 = as.data.table(shift(Promotions_df, n=4L, fill=NA, type=c("lag"), give.names=TRUE))
  
  #Appending new lagged cols
  Data_final <- cbind(Data_final,lag_1,lag_2,lag_3,lag_4)
  
  #PF
  Data_final <- PF_dataprep(GC, Data_final)
  
  #change1
  Data_final[, which(duplicated(names(Data_final))) := NULL]
  
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
  trend_var <- tryCatch({Trend_cal(Data_final[,MODELLED_QTY])}, error = function(e){trend_var <- c(rep(median(Data_final[,MODELLED_QTY])), c(nrow(Data_final)))})
  
  #Padding leading zeros with zero trend, if dropped
  if(nrow(Data_final) - length(trend_var) != 0)
  {
    pad <- c(nrow(Data_final) - length(trend_var))
    trend_var <- c(rep(0, pad), trend_var)
  }
  
  Data_final[,trend_var := trend_var]
  
  #Anomaly Detection
  Data_final <- Anomaly_detection(Data_final)
  
  #Final cleaning
  Data_final[is.na(Data_final)] <- 0
  
  system(paste("echo Function call", GC))
  
  return (Data_final)
}

#Cannibalization dataprep
Cann_dataprep = function(GC, data_new)
{
  #Extracting FOCUS_GC that cannibalizes our GC, take this from the og table
  Cannibalizing_GC <- Base_cann_data[J(GC) ,nomatch = 0L] #set correlation in the dataload
  
  if(dim(Cannibalizing_GC)[1] == 0)#No Cann check
  {
    return(data_new)
  }
  
  for(k in 1:dim(Cannibalizing_GC)[1])
  {
    
    #Subsetting the entire base data 
    Cann_values <- Base_dept_data[J(Cannibalizing_GC$FOCUS_GC[k]) ,nomatch = 0L]
    
    #Skip if empty
    if(dim(Cann_values)[1] == 0)
    {
      next
    }
    
    #Cleaning the cann values
    Cann_values_final <- Data_clean(Cann_values, calendar = calendar)
    rownames(Cann_values_final) <- NULL
    
    #Taking needed promotions that cannibalize the gc
    Promotions <- Cannibalizing_GC[FOCUS_GC == Cannibalizing_GC$FOCUS_GC[k]]$CANNIBALIZING_PROMOTIONS
    
    Promotions <- gsub("'", "", Promotions)
    Promotions <- strsplit(Promotions, ",")[[1]]
    Promotions <- gsub(" ", "", Promotions)
    
    #Renaming values
    temp <- Cann_values_final[, ..Promotions]
    colnames(temp) <- lapply(length(colnames(temp)) , function(x) paste(unique(Cann_values_final$GROUP_CODE), colnames(temp), 'CANN_PROMO',sep='_'))[[1]]
    
    #Adding the Cann values to the base
    data_new <- cbind(data_new , temp)
  }
  
  data_new[is.na(data_new)] <- 0
  
  return(data_new)
  
}

#Halo Dataperp
Halo_dataprep=function(GC, data_new)
{
  
  #Extracting FOCUS_GC that halos our Affined_GC, take this from the og table
  Haloing_GC <- Base_halo[J(unique(AFFINED_DEPT_NBR), GC), nomatch=0L]
  
  if(dim(Haloing_GC)[1] == 0)
  {
    return(data_new)
  }
  
  for(k in 1:dim(Haloing_GC)[1])
  {
    
    #Subsetting the entire base data 
    Halo_values <- Base_data_GC[J(Haloing_GC$FOCUS_GC[k]) ,nomatch = 0L]
    
    #Skip if empty or if the GC is across many depts
    if(dim(Halo_values)[1] == 0 | length(unique(Halo_values$DEPT_NBR)) != 1)
    {
      next
    }
    
    #Cleaning the Halo values
    Halo_values <- Data_clean(Halo_values, calendar = calendar)
    rownames(Halo_values) <- NULL
    
    #Taking needed promotions that halo the focus
    Promotions <- Haloing_GC[FOCUS_GC == Haloing_GC$FOCUS_GC[k]]$HALOING_PROMOTIONS
    
    Promotions <- gsub("'", "", Promotions)
    Promotions <- strsplit(Promotions, ",")[[1]]
    Promotions <- gsub(" ", "", Promotions)
    
    #Renaming values
    temp <- Halo_values[, ..Promotions]
    colnames(temp) <- lapply(length(colnames(temp)) , function(x) paste(unique(Halo_values$GROUP_CODE), colnames(temp), 'HALO_PROMO',sep='_'))[[1]]
    
    data_new <- cbind(data_new , temp)
  }
  
  data_new[is.na(data_new)] <- 0
  
  return(data_new)
  
}

#PF Dataprep
PF_dataprep=function(GC, data_new)
{
  #Fetching GC PF data
  GC_PF_data <- Base_dept_PF[J(GC) ,nomatch = 0L]
  
  if(dim(GC_PF_data)[1] == 0)#No PF check
  {
    return(data_new)
  }
  
  #PF promotions
  Promotions <- GC_PF_data$PF_PROMOTIONS
  
  Promotions <- gsub("'", "", Promotions)
  Promotions <- strsplit(Promotions, ",")[[1]]
  Promotions <- gsub(" ", "", Promotions)
  
  #All lag vars
  Lag_var <- colnames(data_new)[grep("_lag_",colnames(data_new))]
  
  #Lag vars to be removed
  Lag_remove <- setdiff(Lag_var, Promotions)
  
  #Dropping from data
  data_new <- data_new[, c(Lag_remove):=NULL]
  
  #Removing top 4 rows (/weeks)
  data_new <- data_new[-(1:4),]
  
  data_new[is.na(data_new)] <- 0
  rownames(data_new) <- NULL
  
  return(data_new)
  
}

#Data Cleaning 
Data_clean = function(Data_unclean, calendar)
{
  colnames(Data_unclean)<- toupper(colnames(Data_unclean))
  
  #Defining the region
  Data_unclean[, WM_REGION:='NATIONAL']
  
  #Removing factor in page location
  Data_unclean$PAGE_LOCATION <- as.character(Data_unclean$PAGE_LOCATION)
  
  #Setting the null values
  Data_unclean <- Data_unclean[calendar, on ='WM_YR_WK']
  Data_unclean[is.na(Data_unclean)] <- 0
  
  Data_unclean$GROUP_CODE  = sort(unique(Data_unclean$GROUP_CODE),  decreasing = TRUE)[1]
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
                      , use = "pairwise.complete.obs"
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

#Trend calculation
Trend_cal = function(y)
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

#Anomaly Detection
Anomaly_detection = function(data_orig)
{
  conf_level <- 2
  outliers <- vector()
  promo_points <- vector()
  
  #Detrending
  ts_MODELLED_QTY <- data_orig[,MODELLED_QTY] - data_orig[,trend_var]
  
  #Removing seasonality wks if it exists
  if(nrow(data_orig) >= 104)
  {
    lm.ses <- lm(ts_MODELLED_QTY~.,data = data_orig[,grep('week', names(data_orig)), with = FALSE])
    ts_MODELLED_QTY <- ts_MODELLED_QTY - lm.ses$fitted.values
  }
  
  #Defining the weeks where even if promo is present, we consider anomalies
  #Anomalous_promo <- c(12005,12006,12007, 12008, 12009, 12010, 12011, 12012,12013)
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
  #Anomalous_weeks <- data_orig$WM_YR_WK[(which(data_orig$WM_YR_WK >= 12007 & data_orig$WM_YR_WK <= 12020))]
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
    data_orig$MODELLED_QTY[outlier_covid[i]] = median_val + data_orig[,trend_var][outlier_covid[i]] + lm.ses$fitted.values[outlier_covid[i]]
  }
  
  return(data_orig)
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

#Sparsity Condition
Sparse_func <- function(Sparse_df)
{
  
  Sparse_Data <- data.frame(matrix(ncol = ncol(Main_File), nrow = nrow(Sparse_df)))
  colnames(Sparse_Data) = names(Main_File)
  
  #Assinging columns
  Sparse_Data$WM_YR_WK <- Sparse_df$WM_YR_WK
  Sparse_Data$GROUP_CODE <- Sparse_df$GROUP_CODE
  Sparse_Data$WM_REGION <- Sparse_df$WM_REGION
  Sparse_Data$COMPASS_MODELLED_QTY <- Sparse_df$QTY
  Sparse_Data$COMPASS_TOTAL_QTY <- Sparse_df$QTY
  Sparse_Data$COMPASS_TOTAL_SALES <- Sparse_df$SALES
  
  
  Sparse_Data[is.na(Sparse_Data)] <- 0
  
  Sparse_Data$COMPASS_BASELINE_QTY <- rep(median(Sparse_Data$COMPASS_MODELLED_QTY), times = length(Sparse_Data$COMPASS_MODELLED_QTY))
  Sparse_Data$COMPASS_BASELINE_SALES <- Sparse_Data$COMPASS_BASELINE_QTY * Sparse_df$REG_PRICE
  Sparse_Data$COMPASS_SALES_PROMOLIFT <- Sparse_Data$COMPASS_TOTAL_SALES - Sparse_Data$COMPASS_BASELINE_SALES
  Sparse_Data$COMPASS_SALES_PROMOLIFT_PERCENT <- Sparse_Data$COMPASS_SALES_PROMOLIFT/Sparse_Data$COMPASS_BASELINE_SALES
  Sparse_Data$FITTED_VALUES <- Sparse_Data$COMPASS_BASELINE_QTY
  
  return(Sparse_Data)
}

#VIF reduction function 
#One idea , is even if promo is max vif, drop the second highest 
Vif_reduction = function(data_ols, focus_variables)
{
  ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
  
  temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
  temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
  temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
  temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
  data_ols <- cbind(MODELLED_QTY=data_ols$MODELLED_QTY, data_ols[, ..temp_var_ols_vs])
  ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
  
  ols_rem <- names(which(is.na(ols_varSelect$coefficients)))
  ols_rem <- String_clean(ols_rem , data_ols)
  
  if(length(ols_rem)!=0)
  {
    data_ols[, (ols_rem):=NULL]
    
    if(ncol(data_ols)==1)
    {
      colnames(data_ols) <- MODELLED_QTY
    }
    
    ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
    
    temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
    temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
    temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
    temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
    data_ols <- cbind(MODELLED_QTY=data_ols$MODELLED_QTY, data_ols[, ..temp_var_ols_vs])
    ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
    #break
  }
  
  if(ncol(data_ols) >= 3)
  {
    max_vif <- max(car::vif(ols_varSelect))
    
    while(max_vif >= 5)#This is the max VIF we support in our model
    {
      #print('inloop')
      Vif <- car::vif(ols_varSelect)
      max_vif <- max(Vif)
      
      max_vif_var <- names(which(Vif == max_vif))
      max_vif_var <- gsub("`", "", max_vif_var)
      
      #If both values in max_vif_var same vif, drop 1st one and
      if(length(max_vif_var)>1)
      {
        Var_to_remove = names(which(Vif == max_vif))[1]
        Var_to_remove <- String_clean(Var_to_remove , data_ols)
        
        #Dropping the discarded variables
        data_ols[, (Var_to_remove):=NULL]
        
        #Fitting the lm again
        ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
        
        temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
        temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
        temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
        temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
        data_ols <- cbind(MODELLED_QTY=data_ols$MODELLED_QTY, data_ols[, ..temp_var_ols_vs])
        ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
        
        ols_rem <- names(which(is.na(ols_varSelect$coefficients)))
        ols_rem <- String_clean(ols_rem , data_ols)
        
        if(ncol(data_ols) <= 2)
        {
          break
        }
        
        Vif <- car::vif(ols_varSelect)
        max_vif <- max(Vif)
        
        next
      }
      
      toComp <- names(sort(Vif[which(Vif >= (max_vif - 0.1*max_vif))], decreasing = TRUE))
      for(cnt in 1:length(toComp))
      {
        temp_toComp <- toComp[cnt]
        toComp[cnt] <- gsub("`", "", temp_toComp)
      }
      
      #Check if the focus variables are present
      if(any(toComp %in% focus_variables))
      {
        if(all(toComp %in% focus_variables))
        {
          Var_to_remove <- toComp[1]
          data_ols[, (Var_to_remove):=NULL]
          
          #Fitting the lm again
          ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
          
          temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
          temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
          temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
          temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
          data_ols <- cbind(MODELLED_QTY=data_ols$MODELLED_QTY, data_ols[, ..temp_var_ols_vs])
          ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
          
          ols_rem <- names(which(is.na(ols_varSelect$coefficients)))
          ols_rem <- String_clean(ols_rem , data_ols)
          
          if(length(ols_rem)!=0)
          {
            data_ols[, (ols_rem):=NULL]
            
            if(ncol(data_ols)==1)
            {
              colnames(data_ols) <- MODELLED_QTY
            }
            
            ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
            
            temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
            temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
            temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
            temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
            data_ols <- cbind(MODELLED_QTY=data_ols$MODELLED_QTY, data_ols[, ..temp_var_ols_vs])
            ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
          }
          
          if(ncol(data_ols) <= 2)
          {
            break
          }
          
          Vif <- car::vif(ols_varSelect)
          max_vif <- max(Vif)
          
          next
        }
        
        #Drop only the non promo variables
        Var_to_remove <- toComp[which(!(toComp %in% focus_variables))]
        
        data_ols_copy <- data_ols
        #for (col in Var_to_remove) set(data_ols_copy, j=col, value=as.numeric(data_ols_copy[[col]]))
        
        cor_mat <- cor2(cbind(data_ols$y, data_ols_copy[,..Var_to_remove]))
        max_cor_var <- names(which(cor_mat[1,]==max(cor_mat[1,-1])))
        
        #If the highest VIF is also the most correlated drop the second highest
        if(!(is.null(max_vif_var)) & !(is.null(max_cor_var)))
        {
          if(!(is.na(toComp[1])) & max_vif_var==max_cor_var & length(toComp)>1)
          {
            Var_to_remove <- Var_to_remove[2]
          }
        }
        
        
        #Dropping the highest VIF
        if((is.na(toComp[1])))
        {
          Var_to_remove <- max_vif_var
        }else
        {
          Var_to_remove <- Var_to_remove[1]
        }
        
        #Dropping the discarded variables
        Var_to_remove <-  gsub("`", "", Var_to_remove)
        data_ols[, (Var_to_remove):=NULL]
        
        #Fitting the lm again
        ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
        
        temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
        temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
        temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
        temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
        data_ols <- cbind(MODELLED_QTY=data_ols$MODELLED_QTY, data_ols[, ..temp_var_ols_vs])
        ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
        
        ols_rem <- names(which(is.na(ols_varSelect$coefficients)))
        ols_rem <- String_clean(ols_rem , data_ols)
        
        if(ncol(data_ols) <= 2)
        {
          break
        }
        
        Vif <- car::vif(ols_varSelect)
        max_vif <- max(Vif)
        
        next
        
      }
      
      #Else drop on basis of regular correlation
      
      #Convert toComp cols to numeric before feeding into cor()
      #for (col in toComp) set(data_ols_copy, j=col, value=as.numeric(data_ols_copy[[col]]))
      data_ols_copy <- data_ols
      
      cor_mat <- cor2(cbind(data_ols$MODELLED_QTY, data_ols_copy[, ..toComp]))
      max_cor_var <- names(which(cor_mat[1,]==max(cor_mat[1,-1])))
      
      #If the highest VIF is also the most correlated drop the second highest
      if(!(is.null(max_vif_var)) & !(is.null(max_cor_var))){
        if(!(is.na(toComp[1])) & max_vif_var==max_cor_var & length(toComp)>1)
        {
          Var_to_remove <- toComp[2]
        }
      }
      
      #Dropping the highest VIF
      if((is.na(toComp[1])))
      {
        Var_to_remove <- max_vif_var
      }
      else
      {
        Var_to_remove <- toComp[1]
      }
      
      #Dropping the discarded variables
      Var_to_remove <- gsub("`", "", Var_to_remove)
      data_ols[, (Var_to_remove):=NULL]
      
      #Fitting the lm again
      ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
      
      temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
      temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
      temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
      temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
      data_ols <- cbind(MODELLED_QTY=data_ols$MODELLED_QTY, data_ols[, ..temp_var_ols_vs])
      ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
      
      ols_rem <- names(which(is.na(ols_varSelect$coefficients)))
      ols_rem <- String_clean(ols_rem , data_ols)
      
      if(length(ols_rem)!=0)
      {
        data_ols[, (ols_rem):=NULL]
        
        if(ncol(data_ols)==1)
        {
          colnames(data_ols) <- MODELLED_QTY
        }
        
        ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
        
        temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
        temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
        temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
        temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
        data_ols <- cbind(MODELLED_QTY=data_ols$MODELLED_QTY, data_ols[, ..temp_var_ols_vs])
        ols_varSelect <- lm(MODELLED_QTY~.,data = data_ols)
      }
      
      Vif <- car::vif(ols_varSelect)
      max_vif <- max(Vif)
      
      if(ncol(data_ols) <= 2)
      {
        break
      }
      
    }
  }
  
  #Final return of values except intercept
  names <- names(ols_varSelect$coefficients)[-1]
  names <- String_clean(names , data_ols)
  
  return(names)
}

#String Cleaning Function
String_clean = function(names, data)
{
  data_orig_local <- copy(data)
  #Cleaning only values not in ols
  colnames_data <- colnames(data)
  unclean_weeks <- names[grep("week",names)]
  
  #clean_weeks <- sub('1$', "", unclean_weeks)
  #clean_weeks <- sub('_$', "_1",clean_weeks)
  #clean_weeks <- gsub("`", "", clean_weeks)
  
  clean_weeks <- unclean_weeks
  
  names <- names[!(names %in% unclean_weeks)]
  names <- c(names, clean_weeks)
  
  names_clean <- colnames(data)[colnames(data) %in% names]
  names_notclean <- names[!(names %in% names_clean)]
  
  if(length(names_notclean) != 0)
  {
    names_notclean <- sub('1$', "", names_notclean)
    names_notclean <- sub('_$', "_1",names_notclean)
    names_notclean <- gsub("`", "", names_notclean)
    
    if(any(substr(names_notclean, start = 1, stop = 2) %in% 'X.'))
    {
      names_notclean <- gsub('X.', "", names_notclean)
      names_notclean <- gsub("\\.$", "", names_notclean)
    }
    
    names <- c(names_notclean, names_clean)
    
    colnames(data) <- c(names[! names %in% c('(Intercept)')],"MODELLED_QTY")
    setcolorder(data, colnames(data_orig_local))
    
    if('(Intercept)' %in% names){
      names <- c(names[! names %in% c('MODELLED_QTY')])
    } else {
      names <- c('(Intercept)',names[! names %in% c('MODELLED_QTY')])
    }
    
    
  }else
  {
    names <- names_clean
  }
  
  return(names)
  
}

#Baseline function
Baseline_fun = function(data, log_file, GC)#Note we should send the values we want extracted in this function
{
  
  data_orig <- as.data.table(data)
  data_orig[, which(duplicated(names(data_orig))) := NULL]
  
  log_file <- log_file
  data_orig_copy <- data_orig
  
  #Variable definitions
  Colnames_orig <- colnames(data_orig)
  
  Lag_var <- colnames(data_orig)[grep("_lag_",colnames(data_orig))]
  Promo_fixedvars <- c("PA_WEIGHTED_REG","AR_WEIGHTED_REG","TR_WEIGHTED_REG")
  Flag_var <- c("PAGE_LOCATIONBACK","PAGE_LOCATIONFRONT","PAGE_LOCATIONINSIDE","CPP_FLAG","DIGITAL_FLAG","DIGEST_FLAG")
  Halocan_var <- Colnames_orig[grep("_HALO_|_CANN_",Colnames_orig)]
  Cann_var <- Colnames_orig[grep("_CANN_",Colnames_orig)]
  Halo_var <- Colnames_orig[grep("_HALO_",Colnames_orig)]
  Weeks <- Colnames_orig[grep("week",Colnames_orig)]
  Holidays <- c(Colnames_orig[grep("_DAY",Colnames_orig)], 'GOOD_FRIDAY', 'NEW_YEARS', 'ANNIVERSARY_OF_THE_STATUE_OF_WESTMINISTER', 'HALLOWEEN')
  Other_var <- c("MODELLED_QTY", "trend_var", "REG_PRICE") #Note make this caps
  Promos <- c(Promo_fixedvars, Flag_var, Halocan_var)
  
  #Subsetting data and dropping constant columns
  data_orig <- data_orig[,c(..Other_var, ..Promo_fixedvars, ..Halocan_var, ..Flag_var, ..Weeks, ..Holidays, ..Lag_var)]
  data_orig <- data_orig[ , lapply(.SD, function(v) if(uniqueN(v) > 1) v)]
  data_orig <- as.data.table(sapply(data_orig, as.character))
  data_orig <- as.data.table(sapply(data_orig, as.numeric))
  
  
  #Taking the correlation of everything
  cor_matrix <- cor(data_orig[, !c('MODELLED_QTY')])
  pairs_mat <- cor_matrix %>%
    as.data.frame() %>%
    dplyr::mutate(var1 = rownames(.)) %>%
    tidyr::gather(var2, value, -var1) %>%
    arrange(desc(value))
  
  
  #Subsetting values only above 0.9 correlation
  pairs_mat <- pairs_mat[which(abs(pairs_mat$value) >= 0.9 & (pairs_mat$var1 != pairs_mat$var2)),]
  
  #Creating clusters based upon graph theory
  #If variables are part of the same graph path, they are considered one cluster
  list1 = list()
  Correlated_promos <- list()
  var_groups <- list()
  var_groups_orig <- list()
  
  if(dim(pairs_mat)[1]!=0)
  {
    for(j in 1:nrow(pairs_mat))
    {
      list1[[j]] = c(pairs_mat[j,1], pairs_mat[j,2])
      list1[[j]] = sort(list1[[j]])
    }
    
    list1 = unique(list1)
    i = rep(1:length(list1), lengths(list1)) 
    j = factor(unlist(list1))
    tab = sparseMatrix(i = i, j = as.integer(j), x = TRUE, dimnames = list(NULL, levels(j)))
    connects = tcrossprod(tab, boolArith = TRUE)
    group = clusters(graph_from_adjacency_matrix(as(connects, "lsCMatrix")))$membership
    var_groups_orig = tapply(list1, group, function(x) sort(unique(unlist(x))))
    var_groups_orig <- as.list(var_groups_orig)
    var_groups <- var_groups_orig
    
    #Taking into account the various correlations that need to happen
    #Current rules
    #1. Self promotions have the greatest weight 
    #2. Two correlated promotions have effects split equally wo RF
    #3. Seasonality vs promos goes to promos
    #4. Halo/Can and seasonality is split on RF
    
    #Weighting the individual values - Order of precedence: Self-promo, Halo, Cann (homogeneity) + pick anyone from group
    for(i in 1:length(var_groups))
    {
      if(any(var_groups[[i]] %in% Promos))
      {
        #Self-promos
        if(any(var_groups[[i]] %in% c(Promotions)))
        {
          #Number of self-promos > 1
          if(sum(which(var_groups[[i]] %in% c(Promo_fixedvars, Flag_var))) >= 2)
          {
            Intersection <- dplyr::intersect(c(Promo_fixedvars, Flag_var), var_groups[[i]]) #Seeing what promotions are present
            Correlated_promos[i] <- i
            Correlated_promos[[i]] <-  c(Intersection) 
            var_groups[[i]] <- Intersection[1] #Assiging the first one to the group as they can be treated as the same
            
            next
          }
          var_groups[[i]] <- dplyr::intersect(c(Promo_fixedvars, Flag_var), var_groups[[i]])
        }
        
        #If only Halo
        else if(any(var_groups[[i]] %in% Colnames_orig[grep("_HALO_",Colnames_orig)]) && !any(var_groups[[i]] %in% Colnames_orig[grep("_CANN_",Colnames_orig)]))
        {
          
          if(sum(which(var_groups[[i]] %in% Colnames_orig[grep("_HALO_",Colnames_orig)])) >= 2)
          {
            Intersection <- dplyr::intersect(Colnames_orig[grep("_HALO_",Colnames_orig)], var_groups[[i]]) 
            Correlated_promos[i] <- i
            Correlated_promos[[i]] <-  c(Intersection) 
            var_groups[[i]] <- Intersection[1] 
            
            next
          }
          var_groups[[i]] <- dplyr::intersect(Colnames_orig[grep("_HALO_",Colnames_orig)], var_groups[[i]])
        }
        
        #If only Cann (not HALO)
        else if(any(var_groups[[i]] %in% Colnames_orig[grep("_CANN_",Colnames_orig)]) && !any(var_groups[[i]] %in% Colnames_orig[grep("_HALO_",Colnames_orig)]))
        {
          if(sum(which(var_groups[[i]] %in% Colnames_orig[grep("_CANN_",Colnames_orig)])) >= 2)
          {
            Intersection <- dplyr::intersect(Colnames_orig[grep("_CANN_",Colnames_orig)], var_groups[[i]]) 
            Correlated_promos[i] <- i
            Correlated_promos[[i]] <-  c(Intersection) 
            var_groups[[i]] <- Intersection[1] 
            
            next
          }
          var_groups[[i]] <- dplyr::intersect(Colnames_orig[grep("_CANN_",Colnames_orig)], var_groups[[i]])
        }
        
        #PF
        else if(any(var_groups[[i]] %in% c(Lag_var)))
        {
          #Number of self-promos > 1
          if(sum(which(var_groups[[i]] %in% c(Lag_var))) >= 2)
          {
            Intersection <- dplyr::intersect(c(Lag_var), var_groups[[i]]) #Seeing what promotions are present
            Correlated_promos[i] <- i
            Correlated_promos[[i]] <-  c(Intersection) 
            var_groups[[i]] <- Intersection[1] #Assiging the first one to the group as they can be treated as the same
            
            next
          }
          var_groups[[i]] <- dplyr::intersect(c(Lag_var), var_groups[[i]])
        }
        
        #Both Halo and Cann - pick Halo
        else
        {
          if(sum(which(var_groups[[i]] %in% Colnames_orig[grep("_HALO_",Colnames_orig)])) >= 2)
          {
            Intersection <- dplyr::intersect(Colnames_orig[grep("_HALO_",Colnames_orig)], var_groups[[i]]) 
            Correlated_promos[i] <- i
            Correlated_promos[[i]] <-  c(Intersection) 
            var_groups[[i]] <- Intersection[1] 
            
            next
          }
          var_groups[[i]] <- dplyr::intersect(Colnames_orig[grep("_HALO_",Colnames_orig)], var_groups[[i]])
        }
      }
      
    }
    
    #Getting every combination of variable possible, this is a quick check for very large frames
    if(sum(as.numeric(sapply(var_groups, length))) * length(var_groups) > 2)
    {
      result <- purrr::map(var_groups, 1)
      result <- as.data.frame(t(unlist(result)))
    }else
    {
      result <- expand.grid(var_groups)
    }
    
    RF_list <- list()
    ###change - include trend_var: check syntax
    data_noncor <- data_orig[, -dplyr::union(unlist(var_groups_orig), unlist(Correlated_promos)), with = FALSE] #Data without the correlated variables
    
    ##Start of RF
    for(i in 1:nrow(result))
    {
      data_temp <- cbind(data_noncor, data_orig[, unlist(result[i,]), with = FALSE])
      
      Rf <- ranger(dependent.variable.name = 'MODELLED_QTY', data = data_temp, num.trees = 1000, mtry = ncol(data_temp)/3, importance = 'permutation')
      Rf_2 <- data.frame(Rf$variable.importance)
      
      RF_list[[i]] <- Rf_2
    }
    
  }else
  {
    data_noncor <- data_orig 
    
    RF_list <- list()
    Rf <- ranger(dependent.variable.name = 'MODELLED_QTY', data = data_orig, num.trees = 1000, mtry = ncol(data_orig)/3, importance = 'permutation')
    Rf_2 <- data.frame(Rf$variable.importance)
    
    RF_list[[1]] <- Rf_2
  }
  
  
  #Fast aggregation across all dataframes, taking the mean
  l <- lapply(RF_list, function(x) {x$RowName <- row.names(x) ; x})
  Res <- Reduce(function(...) merge(..., by = "RowName", all = TRUE), l)
  
  Rf_2 <- dcast(melt(setDT(Res), "RowName"), 
                RowName ~ sub("\\..*", "", variable), 
                mean, 
                na.rm = TRUE, 
                value.var = "value")
  
  #Taking the best variable from each group, if there are groups
  if(dim(pairs_mat)[1] != 0)
  {
    for(bv in 1:nrow(var_groups))
    {
      comp <- var_groups[bv]
      comp <- unlist(strsplit(comp[[1]], '"'))#To change
      temp <- Rf_2[which(Rf_2$RowName %in% comp)]
      keep_var <- Rf_2$RowName[Rf_2$Rf == max(temp$Rf)]#Keeping the variable that explains the most
      rem_var <- comp[which(comp != keep_var)]
      
      #Dropping all not needed vars from RF
      Rf_2 <- Rf_2[!(Rf_2$RowName %in% unlist(rem_var))]
    }
    
    #Fitting the RF again without the correlated variables
    temp_var <- c(Rf_2$RowName, 'MODELLED_QTY')
    data_temp <- data_orig[,c(..temp_var)]
    
    RF_list <- list()
    
    Rf <- ranger(dependent.variable.name = 'MODELLED_QTY', data = data_temp, num.trees = 1000, mtry = ncol(data_temp)/3, importance = 'permutation')
    Rf_2 <- data.frame(Rf$variable.importance)
    
    RF_list[[1]] <- Rf_2
    
    #Fast aggregation across all dataframes, taking the mean, not needed here , but code does not work wo it
    l <- lapply(RF_list, function(x) {x$RowName <- row.names(x) ; x})
    Res <- Reduce(function(...) merge(..., by = "RowName", all = TRUE), l)
    
    Rf_2 <- dcast(melt(setDT(Res), "RowName"), 
                  RowName ~ sub("\\..*", "", variable), 
                  mean, 
                  na.rm = TRUE, 
                  value.var = "value")
  }
  
  #Change this to optimize
  Rf_2 <- na.omit(Rf_2) 
  Rf_2 <- Rf_2[which(Rf_2$Rf >= 0)]
  Rf_2 <- Rf_2[order(-Rf)]
  
  Rf_2$Rf <- Rf_2$Rf/sum(Rf_2$Rf)
  Rf_2$Rf <- cumsum(Rf_2$Rf)
  temp <- which(Rf_2$Rf > 0.95)[1] #Taking variables that together explain more than 99.5% varience from RF
  
  if(is.na(temp))
  {
    final_var <- 'trend_var'
    
    if(!('trend_var' %in% colnames(data_orig)))
    {
      final_var <- c()
    }
    
  }else
  {
    final_var <- Rf_2[1:temp]$RowName 
  }
  
  
  #Readding the dependent variable
  final_var <- c(final_var, 'MODELLED_QTY')
  
  #Subsetting the data
  data_orig <- data_orig[, ..final_var]
  
  ##VIF loop
  #Vif_var <- Vif_reduction(data_orig , Promotions)
  Vif_var <- tryCatch(
    {
      Vif_reduction(data_orig , Promotions)
    },
    error = function(e){
      Vif_var <- colnames(data_orig[, !c('MODELLED_QTY')]) 
    }
  )
  Vif_var <- c(Vif_var, 'MODELLED_QTY')
  
  #Subsetting the data
  Vif_var <- c(Vif_var[! Vif_var %in% c('(Intercept)')]) ##
  data_orig <- data_orig[, ..Vif_var]
  
  #Readding the promotional, Halo, Cann, Lag variables, that are not correlated
  #Self-promo
  Promotions_readd_noncor <- colnames(data_noncor)[which(colnames(data_noncor) %in% Promotions)]
  Promotions_readd_cor <- as.character(unlist(var_groups)[which(unlist(var_groups) %in% Promotions)])
  Promotions_readd <- c(Promotions_readd_noncor, Promotions_readd_cor)
  
  #Halo
  Halo_readd_noncor <- colnames(data_noncor)[which(colnames(data_noncor) %in% Halo_var)]
  Halo_readd_cor <- as.character(unlist(var_groups)[which(unlist(var_groups) %in% Halo_var)])
  Halo_readd <- c(Halo_readd_noncor, Halo_readd_cor)
  
  #Cann
  Cann_readd_noncor <- colnames(data_noncor)[which(colnames(data_noncor) %in% Cann_var)]
  Cann_readd_cor <- as.character(unlist(var_groups)[which(unlist(var_groups) %in% Cann_var)])
  Cann_readd <- c(Cann_readd_noncor, Cann_readd_cor)
  
  #Lag
  Lag_readd_noncor <- colnames(data_noncor)[which(colnames(data_noncor) %in% Lag_var)]
  Lag_readd_cor <- as.character(unlist(var_groups)[which(unlist(var_groups) %in% Lag_var)])
  Lag_readd <- c(Lag_readd_noncor, Lag_readd_cor)
  
  #Promo_readd_1 <- Promos[which(!(c(Promotions, Halo_var, Cann_var, Lag_var) %in% colnames(data_orig)))]
  Promo_readd_total <- c(Promotions_readd, Halo_readd, Cann_readd, Lag_readd)
  Promo_readd_total <- Promo_readd_total[which(!(Promo_readd_total %in% colnames(data_orig)))]
  
  #Dropping zero columns and constants again
  data_orig <-  cbind(data_orig, data_orig_copy[1:nrow(data_orig)][, ..Promo_readd_total])
  data_orig <- data_orig[ , lapply(.SD, function(v) if(uniqueN(v) > 1) v)]
  
  Vif_var <- tryCatch(
    {
      Vif_reduction(data_orig , Promotions)
    },
    error = function(e){
      Vif_var <- colnames(data_orig[, !c('MODELLED_QTY')]) 
    }
  )
  
  #VIF loop
  #Vif_var <- Vif_reduction(data_orig , Promotions)
  Vif_var <- c(Vif_var, 'MODELLED_QTY')
  
  #Subsetting the data
  Vif_var <- c(Vif_var[! Vif_var %in% c('(Intercept)')]) ##
  data_orig <- data_orig[, ..Vif_var]
  
  ##Final OLS
  ols_varSelect <- lm(MODELLED_QTY~.,data = data_orig)
  
  ##For cases where p>n, drop variables with NA as estimate and refit lm
  temp_ols_vs <- as.data.table(cbind(names(ols_varSelect$coefficients), unname(ols_varSelect$coefficients)))
  temp_var_ols_vs <- temp_ols_vs[complete.cases(temp_ols_vs),]$V1
  temp_var_ols_vs <- gsub("`", "", temp_var_ols_vs)
  temp_var_ols_vs <- c(temp_var_ols_vs[! temp_var_ols_vs %in% c('(Intercept)')])
  data_orig <- cbind(MODELLED_QTY=data_orig$MODELLED_QTY, data_orig[, ..temp_var_ols_vs])
  ols_varSelect <- lm(MODELLED_QTY~.,data = data_orig)
  
  #Dropping -ve HALO vars, in order of significance 
  summary_ols <- as.data.table(summary(ols_varSelect)$coef)
  summary_ols$vars <- String_clean(names(ols_varSelect$coefficients), data_orig)
  summary_ols_halo <- summary_ols[summary_ols$vars %like% "_HALO_", ]
  summary_ols_halo <- summary_ols_halo[summary_ols_halo$Estimate < 0, ]
  summary_ols_halo <- summary_ols_halo[order(summary_ols_halo$`Pr(>|t|)`,decreasing = TRUE), ]
  
  while(nrow(summary_ols_halo)>0)
  {
    data_orig <- data_orig[, c(summary_ols_halo$vars[1]):=NULL]
    ols_varSelect <- lm(MODELLED_QTY~.,data = data_orig)
    summary_ols <- as.data.table(summary(ols_varSelect)$coef)
    summary_ols$vars <- String_clean(names(ols_varSelect$coefficients), data_orig)
    summary_ols_halo <- summary_ols[summary_ols$vars %like% "_HALO_", ]
    summary_ols_halo <- summary_ols_halo[summary_ols_halo$Estimate < 0, ]
    summary_ols_halo <- summary_ols_halo[order(summary_ols_halo$`Pr(>|t|)`,decreasing = TRUE), ]
  }
  
  #Dropping +ve CANN vars, in order of decreasing significance 
  summary_ols <- as.data.table(summary(ols_varSelect)$coef)
  summary_ols$vars <- String_clean(names(ols_varSelect$coefficients), data_orig)
  summary_ols_cann <- summary_ols[summary_ols$vars %like% "_CANN_", ]
  summary_ols_cann <- summary_ols_cann[summary_ols_cann$Estimate > 0, ]
  summary_ols_cann <- summary_ols_cann[order(summary_ols_cann$`Pr(>|t|)`,decreasing = TRUE), ]
  
  while(nrow(summary_ols_cann) > 0)
  {
    data_orig <- data_orig[, c(summary_ols_cann$vars[1]):=NULL]
    ols_varSelect <- lm(MODELLED_QTY~.,data = data_orig)
    summary_ols <- as.data.table(summary(ols_varSelect)$coef)
    summary_ols$vars <- String_clean(names(ols_varSelect$coefficients), data_orig)
    summary_ols_cann <- summary_ols[summary_ols$vars %like% "_CANN_", ]
    summary_ols_cann <- summary_ols_cann[summary_ols_cann$Estimate > 0, ]
    summary_ols_cann <- summary_ols_cann[order(summary_ols_cann$`Pr(>|t|)`,decreasing = TRUE), ]
  }
  
  #Dropping -ve Self-promos, in order of significance
  summary_ols <- as.data.table(summary(ols_varSelect)$coef)
  summary_ols$vars <- String_clean(names(ols_varSelect$coefficients), data_orig)
  summary_ols_op <- summary_ols[summary_ols$vars %like% "^PA_WEIGHTED_REG$|^AR_WEIGHTED_REG$|^TR_WEIGHTED_REG$|^PAGE_LOCATIONBACK$|^PAGE_LOCATIONFRONT$|^PAGE_LOCATIONINSIDE$|^CPP_FLAG$|^DIGITAL_FLAG$|^DIGEST_FLAG$", ]
  summary_ols_op <- summary_ols_op[summary_ols_op$Estimate < 0, ]
  summary_ols_op <- summary_ols_op[order(summary_ols_op$`Pr(>|t|)`,decreasing = TRUE), ]
  
  while(nrow(summary_ols_op) > 0)
  {
    data_orig <- data_orig[, c(summary_ols_op$vars[1]):=NULL]
    ols_varSelect <- lm(MODELLED_QTY~.,data = data_orig)
    summary_ols <- as.data.table(summary(ols_varSelect)$coef)
    summary_ols$vars <- String_clean(names(ols_varSelect$coefficients), data_orig)
    summary_ols_op <- summary_ols[summary_ols$vars %like% "^PA_WEIGHTED_REG$|^AR_WEIGHTED_REG$|^TR_WEIGHTED_REG$|^PAGE_LOCATIONBACK$|^PAGE_LOCATIONFRONT$|^PAGE_LOCATIONINSIDE$|^CPP_FLAG$|^DIGITAL_FLAG$|^DIGEST_FLAG$", ]
    summary_ols_op <- summary_ols_op[summary_ols_op$Estimate < 0, ]
    summary_ols_op <- summary_ols_op[order(summary_ols_op$`Pr(>|t|)`,decreasing = TRUE), ]
  }
  
  #Dropping +ve PF, in order of significance ###change
  summary_ols <- as.data.table(summary(ols_varSelect)$coef)
  summary_ols$vars <- String_clean(names(ols_varSelect$coefficients), data_orig)
  summary_ols_lag <- summary_ols[summary_ols$vars %in% colnames(data_orig)[grep("_lag_",colnames(data_orig))], ]
  summary_ols_lag <- summary_ols_lag[summary_ols_lag$Estimate > 0, ]
  summary_ols_lag <- summary_ols_lag[order(summary_ols_lag$`Pr(>|t|)`,decreasing = TRUE), ]
  
  while(nrow(summary_ols_lag) > 0)
  {
    data_orig <- data_orig[, c(summary_ols_lag$vars[1]):=NULL]
    ols_varSelect <- lm(MODELLED_QTY~.,data = data_orig)
    summary_ols <- as.data.table(summary(ols_varSelect)$coef)
    summary_ols$vars <- String_clean(names(ols_varSelect$coefficients), data_orig)
    summary_ols_lag <- summary_ols[summary_ols$vars %in% colnames(data_orig)[grep("_lag_",colnames(data_orig))], ]
    summary_ols_lag <- summary_ols_lag[summary_ols_lag$Estimate > 0, ]
    summary_ols_lag <- summary_ols_lag[order(summary_ols_lag$`Pr(>|t|)`,decreasing = TRUE), ]
  }
  
  summary_ols <- as.data.table(summary(ols_varSelect)$coef)
  coef_df <- as.data.table(t(ols_varSelect$coef))
  colnames(coef_df) <- gsub("`", "", colnames(coef_df)) ##
  #names(coef_df) <- String_clean(names(ols_varSelect$coefficients), data_orig)
  
  #Create data, coef & contri dfs for Cann and Halo
  #Cann
  Cann_var <- colnames(data_orig)[grep("_CANN_",colnames(data_orig))]
  Cann_df <- data_orig[, ..Cann_var]
  Cann_coef_df <- coef_df[, ..Cann_var]
  Cann_coef_df <- Cann_coef_df[rep(Cann_coef_df[, .I], dim(data_orig)[1])]
  Cann_contri_df <- as.data.table(mapply('*', Cann_coef_df, data_orig_copy[, ..Cann_var]))
  
  #Halo
  Halo_var <- colnames(data_orig)[grep("_HALO_",colnames(data_orig))]
  Halo_df <- data_orig[, ..Halo_var]
  Halo_coef_df <- coef_df[, ..Halo_var]
  Halo_coef_df <- Halo_coef_df[rep(Halo_coef_df[, .I], dim(data_orig)[1])]
  Halo_contri_df <- as.data.table(mapply('*', Halo_coef_df, data_orig_copy[, ..Halo_var]))
  
  #Self Promos
  common_promo <- intersect(Promotions,colnames(data_orig))
  Promos_df <- data_orig[, ..common_promo]
  Promos_coef_df <- coef_df[, ..common_promo]
  Promos_coef_df <- Promos_coef_df[rep(Promos_coef_df[, .I], dim(data_orig)[1])]
  Promos_contri_df <- as.data.table(mapply('*', Promos_coef_df, data_orig_copy[, ..common_promo]))
  
  #PF
  PF_promo <- colnames(data_orig)[grep("_lag_",colnames(data_orig))]
  PF_df <- data_orig[, ..PF_promo]
  PF_coef_df <- coef_df[, ..PF_promo]
  PF_coef_df <- PF_coef_df[rep(PF_coef_df[, .I], dim(data_orig)[1])]
  PF_contri_df <- as.data.table(mapply('*', PF_coef_df, data_orig_copy[, ..PF_promo]))
  
  #Dropping HALO/CANN vars from data_orig and coef and setting Promotions to only self-promos
  data_orig <- data_orig[, c(Halo_var, Cann_var):=NULL]
  coef_df <- coef_df[, c(Halo_var, Cann_var):=NULL]
  
  #Reattribute contris to correlated Promotions, and recreate coef dfs 
  if(length(Correlated_promos)!=0)
  {
    for(i in 1:length(Correlated_promos))
    {
      if(any(Correlated_promos[[i]] %in% c(Promotions, Cann_var, Halo_var, Lag_var)) && any(var_groups[[i]] %in% c(names(coef_df),names(Cann_coef_df), names(Halo_coef_df))))
      {
        #For Cann Promotions
        if(any(Correlated_promos[[i]] %in% Cann_var))
        {
          temp <- which(colnames(Cann_contri_df) %in% var_groups[[i]])
          
          #Splitting it to the new contri based on ratio of promotions
          New_contri <- Cann_contri_df[, ..temp]/length(Correlated_promos[[i]])
          
          #Updating the old (one in the model) contri
          temp_var <- colnames(New_contri)
          temp_x <- unlist(c(New_contri[, ..temp_var]), use.names = FALSE)
          Cann_contri_df[, (temp_var) := temp_x]
          
          #Assigning new contris to correlated Promotions
          corr_prom_colnames <- Correlated_promos[[i]][! Correlated_promos[[i]] %in% c(colnames(Cann_contri_df[, ..temp]))] #subtract original colnames from Correlated_promos[[i]]
          for(cnt in corr_prom_colnames)
          {
            Cann_contri_df[, (cnt) := temp_x]
          }
          
          #Recalculate coef cols
          new_cann_col <- colnames(Cann_contri_df)
          Cann_coef_df <- as.data.table(mapply('/', Cann_contri_df, data_orig_copy[, ..new_cann_col]))
          Cann_coef_df[is.na(Cann_coef_df)] <- 0
          for(cnt in colnames(Cann_coef_df))
          {
            unique_coef <- unique(Cann_coef_df[, ..cnt])
            temp_coef_var <- as.numeric(unique_coef[which(unique(Cann_coef_df[, ..cnt])!=0)][1])#Takes the non na value, assuming 1 unique value apart from NA
            Cann_coef_df[, (cnt) := rep(temp_coef_var, each=nrow(data_orig_copy))] 
            Cann_coef_df[is.na(Cann_coef_df)] <- 0
          }
          
        }
        
        #For Halo Promotions
        if(any(Correlated_promos[[i]] %in% Halo_var))
        {
          temp <- which(colnames(Halo_contri_df) %in% var_groups[[i]])
          
          #Splitting it to the new contri based on ratio of promotions
          New_contri <- Halo_contri_df[, ..temp]/length(Correlated_promos[[i]])
          
          #Updating the old (one in the model) contri
          temp_var <- colnames(New_contri)
          temp_x <- unlist(c(New_contri[, ..temp_var]), use.names = FALSE)
          Halo_contri_df[, (temp_var) := temp_x]
          
          #Assigning new contris to correlated Promotions
          corr_prom_colnames <- Correlated_promos[[i]][! Correlated_promos[[i]] %in% c(colnames(Halo_contri_df[, ..temp]))] #subtract original colnames from Correlated_promos[[i]]
          for(cnt in corr_prom_colnames)
          {
            Halo_contri_df[, (cnt) := temp_x]
          }
          
          #Recalculate coef cols
          new_halo_col <- colnames(Halo_contri_df)
          Halo_coef_df <- as.data.table(mapply('/', Halo_contri_df, data_orig_copy[, ..new_halo_col]))
          Halo_coef_df[is.na(Halo_coef_df)] <- 0
          for(cnt in colnames(Halo_coef_df))
          {
            unique_coef <- unique(Halo_coef_df[, ..cnt])
            temp_coef_var <- as.numeric(unique_coef[which(unique(Halo_coef_df[, ..cnt])!=0)][1]) #Takes the non na value, assuming 1 unique value apart from NA
            Halo_coef_df[, (cnt) := rep(temp_coef_var, each=nrow(data_orig_copy))] 
            Halo_coef_df[is.na(Halo_coef_df)] <- 0
          }
          
        }
        
        #For self Promotions
        if(any(Correlated_promos[[i]] %in% Promotions))
        {
          temp <- which(colnames(Promos_contri_df) %in% var_groups[[i]])
          
          #Splitting it to the new contri based on ratio of promotions
          New_contri <- Promos_contri_df[, ..temp]/length(Correlated_promos[[i]])
          
          #Updating the old (one in the model) contri
          temp_var <- colnames(New_contri)
          temp_x <- unlist(c(New_contri[, ..temp_var]), use.names = FALSE)
          Promos_contri_df[, (temp_var) := temp_x]
          
          #Assigning new contris to correlated Promotions
          corr_prom_colnames <- Correlated_promos[[i]][! Correlated_promos[[i]] %in% c(colnames(Promos_contri_df[, ..temp]))] #subtract original colnames from Correlated_promos[[i]]
          for(cnt in corr_prom_colnames)
          {
            Promos_contri_df[, (cnt) := temp_x]
          }
          
          #Recalculate coef cols
          new_op_col <- colnames(Promos_contri_df)
          Promos_coef_df <- as.data.table(mapply('/', Promos_contri_df, data_orig_copy[, ..new_op_col]))
          Promos_coef_df[is.na(Promos_coef_df)] <- 0
          for(cnt in colnames(Promos_coef_df))
          {
            unique_coef <- unique(Promos_coef_df[, ..cnt])
            temp_coef_var <- as.numeric(unique_coef[which(unique(Promos_coef_df[, ..cnt])!=0)][1]) #Takes the non na value, assuming 1 unique value apart from NA
            Promos_coef_df[, (cnt) := rep(temp_coef_var, each=nrow(data_orig_copy))] 
            Promos_coef_df[is.na(Promos_coef_df)] <- 0
          }
          
          
        }
        
        #For PF
        if(any(Correlated_promos[[i]] %in% Lag_var))
        {
          temp <- which(colnames(PF_contri_df) %in% var_groups[[i]])
          
          #Splitting it to the new contri based on ratio of promotions
          New_contri <- PF_contri_df[, ..temp]/length(Correlated_promos[[i]])
          
          #Updating the old (one in the model) contri
          temp_var <- colnames(New_contri)
          temp_x <- unlist(c(New_contri[, ..temp_var]), use.names = FALSE)
          PF_contri_df[, (temp_var) := temp_x]
          
          #Assigning new contris to correlated Promotions
          corr_prom_colnames <- Correlated_promos[[i]][! Correlated_promos[[i]] %in% c(colnames(PF_contri_df[, ..temp]))] #subtract original colnames from Correlated_promos[[i]]
          for(cnt in corr_prom_colnames)
          {
            PF_contri_df[, (cnt) := temp_x]
          }
          
          #Recalculate coef cols
          new_pf_col <- colnames(PF_contri_df)
          PF_coef_df <- as.data.table(mapply('/', PF_contri_df, data_orig_copy[, ..new_pf_col]))
          PF_coef_df[is.na(PF_coef_df)] <- 0
          for(cnt in colnames(PF_coef_df))
          {
            unique_coef <- unique(PF_coef_df[, ..cnt])
            temp_coef_var <- as.numeric(unique_coef[which(unique(PF_coef_df[, ..cnt])!=0)][1]) #Takes the non na value, assuming 1 unique value apart from NA
            PF_coef_df[, (cnt) := rep(temp_coef_var, each=nrow(data_orig_copy))] 
            PF_coef_df[is.na(PF_coef_df)] <- 0
          }
          
          
        }
        
      }
    }
  }
  
  #String clean for HALO/CANN vars - remove 'X' from colnames for (data)dfs and coef_df, change this, once I get a good example
  if(any(substr(colnames(Halo_df), start = 1, stop = 1) %in% 'X'))
  {
    colnames(Halo_df) <- gsub('X', "", colnames(Halo_df))
    colnames(Halo_coef_df) <- gsub('X', "", colnames(Halo_coef_df))
    colnames(Halo_contri_df) <- gsub('X', "", colnames(Halo_contri_df))
  }
  
  if(any(substr(colnames(Cann_df), start = 1, stop = 1) %in% 'X'))
  {
    colnames(Cann_df) <- gsub('X', "", colnames(Cann_df))
    colnames(Cann_coef_df) <- gsub('X', "", colnames(Cann_coef_df))
    colnames(Cann_contri_df) <- gsub('X', "", colnames(Cann_contri_df))
  }
  
  #Get unique FGCs having CANN/HALO Promotions
  FGC_halo_list <- colnames(Halo_contri_df)
  FGC_halo_list <- unique(vapply(strsplit(FGC_halo_list,"_"), `[`, 1, FUN.VALUE=character(1)))
  
  FGC_cann_list <- colnames(Cann_contri_df)
  FGC_cann_list <- unique(vapply(strsplit(FGC_cann_list,"_"), `[`, 1, FUN.VALUE=character(1)))
  
  
  #get rid of promo cols from coef_df, pick 1st row from promos_coef_df, cbind both
  promo_cols <- colnames(Promos_coef_df)
  pf_cols <- colnames(PF_coef_df)
  coef_df <- coef_df[, c(promo_cols, pf_cols):=NULL]
  coef_df <- cbind(coef_df, Promos_coef_df[1,], PF_coef_df[1,])
  
  #Adding GroupCode to the coef file
  coef_df <- cbind(data_orig_copy$GROUP_CODE[1], coef_df)
  names(coef_df)[1] <- 'GROUP_CODE'
  
  ##Coefficent calculation
  Promos_empty <- setnames(data.table(matrix(nrow = dim(data_orig)[1], ncol = length(Promotions))), c(Promotions))
  Lags_empty <- setnames(data.table(matrix(nrow = dim(data_orig)[1], ncol = length(Lag_var))), c(Lag_var))
  Weeks_empty <- setnames(data.table(matrix(nrow = dim(data_orig)[1], ncol = length(Weeks))), c(Weeks))
  Holidays_empty <- setnames(data.table(matrix(nrow = dim(data_orig)[1], ncol = length(Holidays))), c(Holidays))
  Others_empty <- setnames(data.table(matrix(nrow = dim(data_orig)[1], ncol = 2)), c('trend_var', 'REG_PRICE'))
  Cann_Halo_coef_empty <- setnames(data.table(matrix(nrow = 1, ncol = 12)), c(Promotions,"GROUP_CODE", "FOCUS_GC", "FOCUS_GC_DEPT"))
  
  #Creates a repeating pattern, of dim that we want
  tmp_coef <- coef_df[rep(coef_df[, .I], dim(data_orig)[1])]
  
  ##Halo Coefficents
  halo_fgc_agc_df <- list()
  if(length(FGC_halo_list) != 0)
  {
    cnt <- 1
    for(hgc in FGC_halo_list)
    {
      
      ##Coef cleaning
      halo_fgc_coeff_temp_var <- colnames(Halo_coef_df)[grep(hgc, colnames(Halo_coef_df))]
      if(length(halo_fgc_coeff_temp_var) > 1){
        halo_fgc_coeff_temp_var <- halo_fgc_coeff_temp_var[1]
      }
      halo_fgc_coeff_temp <- Halo_coef_df[, ..halo_fgc_coeff_temp_var]
      colnames(halo_fgc_coeff_temp) <- sub(".+?_", "", colnames(halo_fgc_coeff_temp))
      colnames(halo_fgc_coeff_temp) <- gsub("_HALO_PROMO","",colnames(halo_fgc_coeff_temp))
      #regmatches(x, regexpr(")", x), invert = TRUE)
      
      ##Halo Coefficents
      x <- which(colnames(halo_fgc_coeff_temp) %in% Promotions)
      temp_halo <- colnames(halo_fgc_coeff_temp[,..x])
      
      y <- which(!(Promotions %in% temp_halo))
      temp_nohalo <- colnames(Promos_empty[,..y])
      
      #Subset dataframe that has actual promo values
      Coef_halo <- halo_fgc_coeff_temp[,..temp_halo]
      Coef_halo <- as.data.frame(sapply(Coef_halo, as.numeric))
      
      #Subset empty frame of Promotions not present
      Coef_nohalo <- Promos_empty[,..temp_nohalo]
      Coef_nohalo <- as.data.frame(sapply(Coef_nohalo, as.numeric))
      
      #Cbind to get all promo coefficents 
      Coef_halo <- as.data.table(cbind(as.data.table(Coef_halo), as.data.table(Coef_nohalo)))[1,]
      setcolorder(Coef_halo, Promotions)
      Coef_halo[is.na(Coef_halo)] <- 0
      Coef_halo <- Coef_halo[, lapply(.SD, as.numeric)]
      colnames(Coef_halo) <- lapply(length(colnames(Coef_halo)) , function(x) paste(colnames(Coef_halo), 'FOCUS_COEF',sep='_'))[[1]]
      
      #Final Info
      Coef_halo$GROUP_CODE <- data_orig_copy$GROUP_CODE[1]
      Coef_halo$FOCUS_GC <- hgc
      Coef_halo$FOCUS_GC_DEPT <- temp_Base_halo[temp_Base_halo$FOCUS_GC == hgc]$FOCUS_DEPT_NBR[1]
      
      #Append
      halo_fgc_agc_df[[cnt]] = Coef_halo
      cnt <- cnt + 1
      
    }
    
    halo_fgc_agc_final_df <- rbindlist(halo_fgc_agc_df, use.names = TRUE)
  }else
  {
    halo_fgc_agc_final_df <- Cann_Halo_coef_empty
    colnames(halo_fgc_agc_final_df)[1:9] <- lapply(length(colnames(halo_fgc_agc_final_df)[1:9]) , function(x) paste(colnames(halo_fgc_agc_final_df)[1:9], 'FOCUS_COEF',sep='_'))[[1]]
    
  }
  
  
  ##Cann Coefficents
  cann_fgc_agc_df <- list()
  if(length(FGC_cann_list != 0))
  {
    cnt <- 1
    for(fgc in FGC_cann_list)
    {
      ##Coef cleaning
      cann_fgc_coeff_temp_var <- colnames(Cann_coef_df)[grep(fgc, colnames(Cann_coef_df))]
      if(length(cann_fgc_coeff_temp_var) > 1){
        cann_fgc_coeff_temp_var <- cann_fgc_coeff_temp_var[1]
      }
      cann_fgc_coeff_temp <- Cann_coef_df[, ..cann_fgc_coeff_temp_var]
      colnames(cann_fgc_coeff_temp) <- sub(".+?_", "", colnames(cann_fgc_coeff_temp))
      colnames(cann_fgc_coeff_temp) <- gsub("_CANN_PROMO","",colnames(cann_fgc_coeff_temp))
      
      ##Cann Coefficents
      x <- which(colnames(cann_fgc_coeff_temp) %in% Promotions)
      temp_cann <- colnames(cann_fgc_coeff_temp[,..x])
      
      y <- which(!(Promotions %in% temp_cann))
      temp_nocann <- colnames(Promos_empty[,..y])
      
      #Subset dataframe that has actual promo values
      Coef_cann <- cann_fgc_coeff_temp[,..temp_cann]
      Coef_cann <- as.data.frame(sapply(Coef_cann, as.numeric))
      
      #Subset empty frame of Promotions not present
      Coef_nocann <- Promos_empty[,..temp_nocann]
      Coef_nocann <- as.data.frame(sapply(Coef_nocann, as.numeric))
      
      #Cbind to get all promo coefficents 
      Coef_cann <- as.data.table(cbind(as.data.table(Coef_cann), as.data.table(Coef_nocann)))[1,]
      setcolorder(Coef_cann, Promotions)
      Coef_cann[is.na(Coef_cann)] <- 0
      Coef_cann <- Coef_cann[, lapply(.SD, as.numeric)]
      colnames(Coef_cann) <- lapply(length(colnames(Coef_cann)) , function(x) paste(colnames(Coef_cann), 'FOCUS_COEF',sep='_'))[[1]]
      
      #Final Info
      Coef_cann$GROUP_CODE <- data_orig_copy$GROUP_CODE[1]
      Coef_cann$FOCUS_GC <- fgc
      Coef_cann$FOCUS_GC_DEPT <- Base_dept_data$DEPT_NBR[1]
      
      
      #Append
      cann_fgc_agc_df[[cnt]] = Coef_cann
      cnt <- cnt + 1
      
    }
    cann_fgc_agc_final_df <- rbindlist(cann_fgc_agc_df, use.names = TRUE)
  }else
  {
    cann_fgc_agc_final_df <- Cann_Halo_coef_empty
    colnames(cann_fgc_agc_final_df)[1:9] <- lapply(length(colnames(cann_fgc_agc_final_df)[1:9]) , function(x) paste(colnames(cann_fgc_agc_final_df)[1:9], 'FOCUS_COEF',sep='_'))[[1]]
  }
  
  
  ##Promo Coefficents
  x <- which(colnames(tmp_coef) %in% Promotions)
  temp_promos <- colnames(tmp_coef[,..x])
  
  y <- which(!(Promotions %in% temp_promos))
  temp_nopromo <- colnames(Promos_empty[,..y])
  
  #Subset dataframe that has actual promo values
  Coef_promos <- tmp_coef[,..temp_promos]
  Coef_promos <- as.data.frame(sapply(Coef_promos, as.numeric))
  
  #Subset empty frame of Promotions not present
  Coef_nopromo <- Promos_empty[,..temp_nopromo]
  Coef_nopromo <- as.data.frame(sapply(Coef_nopromo, as.numeric))
  
  #Cbind to get all promo coefficents 
  Coef_df <- as.data.table(cbind(as.data.table(Coef_promos), as.data.table(Coef_nopromo)))
  setcolorder(Coef_df, Promotions)
  Coef_df[is.na(Coef_df)] <- 0
  Coef_df <- Coef_df[, lapply(.SD, as.numeric)]
  colnames(Coef_df) <- lapply(length(colnames(Coef_df)) , function(x) paste(colnames(Coef_df), 'COEF',sep='_'))[[1]]
  
  ##Lag Coefficents
  x <- which(colnames(tmp_coef) %in% Lag_var)
  temp_lags <- colnames(tmp_coef[,..x])
  
  y <- which(!(Lag_var %in% temp_lags))
  temp_nolags <- colnames(Lags_empty[,..y])
  
  #Subset dataframe that has actual lag values
  Coef_lags <- tmp_coef[,..temp_lags]
  Coef_lags <- as.data.frame(sapply(Coef_lags, as.numeric))
  
  #Subset empty frame of lags not present
  Coef_nolags <- Lags_empty[,..temp_nolags]
  Coef_nolags <- as.data.frame(sapply(Coef_nolags, as.numeric))
  
  #Cbind to get all promo lag 
  Coef_lag <- as.data.table(cbind(as.data.table(Coef_lags), as.data.table(Coef_nolags)))
  setcolorder(Coef_lag, Lag_var)
  Coef_lag[is.na(Coef_lag)] <- 0
  Coef_lag <- Coef_lag[, lapply(.SD, as.numeric)]
  
  #Lag Contributions, note we calculate the contribution here due to complexity of doing it in hive, ideally we should not
  Contri_lag <- as.data.table(mapply('*', Coef_lag, data_orig_copy[, ..Lag_var]))
  
  colnames(Coef_lag) <- lapply(length(colnames(Coef_lag)) , function(x) paste(colnames(Coef_lag), 'COEF',sep='_'))[[1]]
  colnames(Contri_lag) <- lapply(length(colnames(Contri_lag)) , function(x) paste(colnames(Contri_lag), 'CONTRI',sep='_'))[[1]]
  
  ##Week Seasonality Coefficents
  x <- which(colnames(tmp_coef) %in% Weeks)
  temp_weeks <- colnames(tmp_coef[,..x])
  
  y <- Weeks[which(!(Weeks %in% temp_weeks))]
  temp_noweek <- colnames(Weeks_empty[,..y])
  
  #Subset dataframe that has actual week values
  Coef_weeks <- tmp_coef[,..temp_weeks]
  
  #Subset empty frame of week not present
  Coef_noweek <- Weeks_empty[,..temp_noweek]
  
  #Cbind to get all week coefficents 
  Coef_week <- as.data.table(cbind(Coef_weeks, Coef_noweek))
  setcolorder(Coef_week, Weeks)
  Coef_week[is.na(Coef_week)] <- 0
  Coef_week <- Coef_week[, lapply(.SD, as.numeric)]
  
  ##Holiday Coefficents
  x <- which(colnames(tmp_coef) %in% Holidays)
  temp_holidays <- colnames(tmp_coef[,..x])
  
  y <- Holidays[which(!(Holidays %in% temp_holidays))]
  temp_noholidays <- colnames(Holidays_empty[,..y])
  
  #Subset dataframe that has actual Holiday values
  Coef_holidays <- tmp_coef[,..temp_holidays]
  
  #Subset empty frame of Holiday not present
  Coef_noholidays <- Holidays_empty[,..temp_noholidays]
  
  #Cbind to get all Holiday coefficents 
  Coef_holidays <- as.data.table(cbind(Coef_holidays, Coef_noholidays))
  setcolorder(Coef_holidays, Holidays)
  Coef_holidays[is.na(Coef_holidays)] <- 0
  Coef_holidays <- Coef_holidays[, lapply(.SD, as.numeric)]
  colnames(Coef_holidays) <- lapply(length(colnames(Coef_holidays)) , function(x) paste(colnames(Coef_holidays), 'COEF',sep='_'))[[1]]
  
  ##Baseline Coefficents
  x <- which(colnames(tmp_coef) %in% Other_var)
  temp_othervar <- colnames(tmp_coef[,..x])
  
  temp <- setdiff(Other_var, 'MODELLED_QTY')
  y <- temp[which(!(temp %in% temp_othervar))]
  temp_no_othervar <- colnames(Others_empty[, ..y])
  
  #Subset dataframe that has actual other values
  Coef_othervar <- tmp_coef[, ..temp_othervar]
  
  #Subset empty frame of other not present
  Coef_no_othervar <- Others_empty[,..temp_no_othervar]
  
  #Cbind to get all other coefficents 
  Coef_othervar <-  as.data.table(cbind(Coef_othervar, Coef_no_othervar))
  setcolorder(Coef_othervar, temp)
  Coef_othervar[is.na(Coef_othervar)] <- 0
  Coef_othervar <- Coef_othervar[, lapply(.SD, as.numeric)]
  
  ##Extracting other columns we need
  Other_df <- data_orig_copy[1:nrow(data_orig)][, c('WM_YR_WK', 'GROUP_CODE', 'QTY', 'MODELLED_QTY')]
  
  #Creating true baseline and few other metrics
  Trend_reg_contri <- as.data.table(mapply('*', Coef_othervar, data_orig_copy[, ..Other_var][,-1]))
  Week_contri <- as.data.table(mapply('*', Coef_week, data_orig_copy[, ..Weeks][, lapply(.SD, as.character)][, lapply(.SD, as.numeric)]))
  Holiday_contri <- as.data.table(mapply('*', Coef_holidays, data_orig_copy[, ..Holidays]))
  
  Contri_final <- cbind(Trend_reg_contri, Week_contri, Holiday_contri,  ols_varSelect$coefficients[1])
  
  #Adding Final Values
  Other_df$COMPASS_BASELINE_QTY <- rowSums(Contri_final)
  Other_df$COMPASS_MODELLED_QTY <- data_orig_copy[1:nrow(data_orig)]$MODELLED_QTY
  Other_df$COMPASS_TOTAL_QTY <- data_orig_copy[1:nrow(data_orig)]$QTY
  Other_df$COMPASS_BASELINE_SALES <- data_orig_copy[1:nrow(data_orig)]$REG_PRICE *  Other_df$COMPASS_BASELINE_QTY
  Other_df$COMPASS_TOTAL_SALES <- data_orig_copy[1:nrow(data_orig)]$SALES
  Other_df$COMPASS_SALES_PROMOLIFT <- data_orig_copy[1:nrow(data_orig)]$SALES - Other_df$COMPASS_BASELINE_SALES
  Other_df$COMPASS_SALES_PROMOLIFT_PERCENT <- Other_df$COMPASS_SALES_PROMOLIFT / Other_df$COMPASS_BASELINE_SALES
  
  
  #Final Dataframe
  Df_results <- cbind(Other_df, Coef_othervar, Coef_df, Coef_week, Coef_holidays, Coef_lag, Contri_lag, tmp_coef$`(Intercept)`)#Add contris and coef, values are coming in
  colnames(Df_results)[ncol(Df_results)] <- 'INTERCEPT'
  Df_results[is.na(Df_results)] <- 0
  
  
  ##Final statistics calculation as well as creation of final dataframe
  ols_fitted <- ols_varSelect$fitted
  
  Final_res <- cbind(Df_results, data_orig_copy[1:nrow(data_orig)][,"WM_REGION"], ols_varSelect$fitted.values)#Recode this bit
  colnames(Final_res)[ncol(Final_res)] <- 'FITTED_VALUES'
  Final_res[is.na(Final_res)] <- 0
  
  ##Model Stats #Note to be rewritten a lot better
  #MAPE
  mape_vec <- abs(Final_res$FITTED_VALUES - Final_res$MODELLED_QTY)/(Final_res$MODELLED_QTY)
  mape_vec_rem <- which(mape_vec=="NaN" | mape_vec=="Inf" | mape_vec=="-Inf")
  
  if(length(mape_vec_rem)!=0)
  {
    mape_vec=mape_vec[-mape_vec_rem]
  }
  
  mape <- mean(mape_vec)
  log_file[1,12] <- mape
  summ_ols <- summary(ols_varSelect)
  
  #Fstats
  if(!is.null(summ_ols$fstatistic[1]))
  {
    log_file[1,13]=summ_ols$fstatistic[1]
  }
  
  if(!is.null(summ_ols$fstatistic[1]))
  {
    log_file[1,14]=pf(summ_ols$fstatistic[1],summ_ols$fstatistic[2],summ_ols$fstatistic[3],
                      lower.tail = FALSE)
  }
  
  #RMSE
  rmse_val <- sum((Final_res$FITTED_VALUES -Final_res$MODELLED_QTY)^2)
  rmse_sku <- sqrt(rmse_val/nrow(data_orig))
  
  log_file[1,15] <- rmse_sku
  
  #AdjRsquare
  if(is.null(summary(ols_varSelect)$adj.r.squared))
  {
    log_file[1,11] = 0
  }else
  {
    log_file[1,11] = summary(ols_varSelect)$adj.r.squared
  }
  
  #Rsquare
  if(is.null(summary(ols_varSelect)$r.squared))
  {
    log_file[1,16] = 0
  }else
  {
    log_file[1,16] = summary(ols_varSelect)$r.squared
  }
  
  
  #Creation of the insights file, leave this as it is maybe, change this in the future
  insights_1 <- data.frame(matrix(nrow = 1,ncol = (length(Promotions)+2)))
  colnames(insights_1) <- c("GROUP_CODE","WM_REGION",c(Promotions))
  
  for(i in 3:ncol(insights_1))
  {
    promo <- paste0(colnames(insights_1)[i],"_coef")
    
    r <- which(promo == colnames(Coef_df))
    if(length(r)==0)
    {
      ret="NA"
    }else
    {
      if(Coef_df[1,r]==0)
      {
        ret="NA/Insignificant"
      }else if(Coef_df[1,r]>0)
      {
        ret="Positive Significant"
      }else
      {
        ret="Negative Significant"
      }
    }
    insights_1[1,i]=ret
  }
  
  insights_1[1,1] <- data_orig_copy$GROUP_CODE[1]
  insights_1[1,2] <- data_orig_copy$WM_REGION[1]
  extvar_insights <- data.frame(matrix(0,nrow=1,ncol=4))
  colnames(extvar_insights) <- c("Positive_Significant","Negative_Significant","Positive_Insignificant","Negative_Insignificant")
  
  pos <- as.numeric()#note change this in the future to correct values
  
  if(length(pos) == 0)
  {
    extvar_insights[1,4] = 0
    extvar_insights[1,3] = 0
    extvar_insights[1,2] = 0
    extvar_insights[1,1] = 0
  }else
  {
    ps=0
    pi=0
    ns=0
    ni=0
    
    for(k in 1:length(pos))
    {
      r=which(rownames(summary_ols)==pos[[k]])
      if(summary_ols[r,1]>0 && summary_ols[r,4]<=0.1)
      {
        ps=ps+1
      }else if(summary_ols[r,1]>0 && summary_ols[r,4]>0.1)
      {
        pi=pi+1
      }else if(summary_ols[r,1]<0 && summary_ols[r,4]<=0.1)
      {
        ns=ns+1
      }else
      {
        ni=ni+1
      }
    }
    
    extvar_insights[1,1]=ps
    extvar_insights[1,2]=ns
    extvar_insights[1,3]=pi
    extvar_insights[1,4]=ni
  }
  
  insights <- cbind(insights_1, extvar_insights)
  
  system(paste("echo Function call", data_orig_copy$GROUP_CODE[1]))
  
  return(list(data_orig_copy$GROUP_CODE[1], insights, log_file, Final_res, halo_fgc_agc_final_df, cann_fgc_agc_final_df))
}

#Starting function
Region_loop = function(GC) 
{
  Data = Data_frame_list_prep[[GC]]
  
  first_val <- which(!Data$MODELLED_QTY <= 0)
  
  #Null Group Code condition
  if(length(first_val) == 0)
  {
    return(list(Insights, main_log, Main_File, Cann_halo_main_File, Cann_halo_main_File))
  }
  
  Data <- Data[c(first_val[1]:nrow(Data)),]
  
  empty_df <- data.frame()
  final_insights <- data.frame()
  
  if(length(Data) != 0)
  {
    set.seed(1001)
    
    log_file <- data.frame(matrix(ncol = 19, nrow = 1))
    colnames(log_file) <- colnames(main_log)
    
    log_file[1,1] <- Data$GROUP_CODE[1]
    log_file[1,2] <- Data$WM_REGION[1]
    log_file[1,10] <- 1
    
    first_val <- which(!Data$MODELLED_QTY <= 0)
    
    #Min number of points for a model to work, plus too much data condition
    if(length(first_val) <= 12)
    {
      log_file[1,3] = "Not enough data"
      
      #Calling Sparsity Function
      Sparse_Data = Sparse_func(Data)
      
      return(list(Insights, log_file, Sparse_Data, Cann_halo_main_File, Cann_halo_main_File))
    }else
    {
      log_file[1,3] = "Quantity sold"
    }
    
    Data <- Data[c(first_val[1]:nrow(Data)),]
    zeroes <- length(which(Data$MODELLED_QTY == 0))
    nonzeroes <- length(which(Data$MODELLED_QTY > 0))
    
    #Sparsity condition 
    if((zeroes/nonzeroes) > 4)
    {
      log_file[1,4] = "Highly sparse data"
      main_log = rbind(main_log, log_file)
      
      #Calling Sparsity Function
      Sparse_Data = Sparse_func(Data)
      
      return(list(Insights, log_file, Sparse_Data, Cann_halo_main_File, Cann_halo_main_File))
    }
    
    log_file[1,3] = "Quantity sold"
    log_file[1,4] = "Data not sparse"
    
    #system(paste("echo Function call start of region loop"))
    
    #Function call to baseline
    ddd <- list()
    tryCatch({
      R.utils::withTimeout({
        skip <- FALSE
        ddd <- tryCatch(
          {
            Baseline_fun(Data, log_file, GC)
          },
          error = function(e){
            skip <<- TRUE
          }
        )
        if(skip){
          system(paste("echo Error GC", unique(Data$GROUP_CODE)[1]))
          
        }
      }, timeout = 3600, elapsed = 3600, cpu =Inf)
    }, TimeoutException = function(ex){next})
    
    
    log_file <- ddd[[3]]
    final_insights <- rbind(final_insights, ddd[[2]])
    main_log <- rbind(main_log, log_file)
    
    if(dim(ddd[[5]])[2] == 1){
      ddd[[5]] = Cann_halo_main_File
    }
    
    if(dim(ddd[[6]])[2] == 1){
      ddd[[6]] = Cann_halo_main_File
    }
    
    return(list(ddd[[2]], ddd[[3]], ddd[[4]], ddd[[5]], ddd[[6]]))
  }else
  {
    data_not_generated <- c(data_not_generated,GC)
    final_insights <- data.frame()
    
    return(list(Insights, main_log, Main_File, Halo_main_File, Cann_main_File))
  }
}


#Department Dec.
#Depts <- "SELECT DISTINCT(DEPT_NBR) FROM dbcure.promox_model_data_national_rollup_mrktng_flyer WHERE DEPT_NBR <> 38 AND DEPT_NBR <> 49 AND DEPT_NBR <> 50 ORDER BY DEPT_NBR"
#Depts <- dataset.load(name = 'GCP DD', query = Depts)
#names(Depts) <- toupper(names(Depts))
#Depts <- as.vector(Depts$DEPT_NBR)
Depts <- c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,38,40,46,72,80,81,82,83,85,87,90,91,92,93,94,95,96,97,98)


#Calendar Data
rolling_weeks <- 203
Forecast_length <- 12
#week_end <- 202346
week_end <- getArgument("week_end","PARAMS")
week_end <- as.integer(week_end)


week_start <- paste("select min(wm_yr_wk) from (select distinct wm_yr_wk from dbcure.promox_marketing_wm_calendar where wm_yr_wk<",week_end," order by wm_yr_wk desc limit ",rolling_weeks,") a")
#week_start <- dataset.load(name = 'GCP DD', query = week_start)
week_start <- contry('load',10,5,week_start)
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

##Aggregate data load (all depts)

#Base data ingestion
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

cols_rnd <- c("PA_WEIGHTED_REG", "AR_WEIGHTED_REG", "TR_WEIGHTED_REG")
Base_data[,(cols_rnd) := round(.SD,1), .SDcols=cols_rnd]
setkey(Base_data, DEPT_NBR, GROUP_CODE)

#GC keyed level table
Base_data_GC <- copy(Base_data)
setkey(Base_data_GC, GROUP_CODE)


#Base Cannibalization Data
query <- paste("SELECT * FROM dbcure.PROMOX_CANN_ANTIGC_SCORECARD_auto WHERE FGC_DEPT_NBR IN (",paste(shQuote(Depts, type="sh"), collapse=", "),") ORDER BY FOCUS_GC, ANTI_GC ")
#Base_cann <- dataset.load(name = 'GCP DD', query = query)
Base_cann <- contry('load',10,5,query)
Base_cann <- as.data.table(Base_cann)
Base_cann <- unique(Base_cann)

names(Base_cann) <- toupper(names(Base_cann))
Base_cann$FOCUS_GC = as.character(Base_cann$FOCUS_GC)
Base_cann$ANTI_GC = as.character(Base_cann$ANTI_GC)
Base_cann$FGC_DEPT_NBR = as.numeric(Base_cann$FGC_DEPT_NBR)
Base_cann$AGC_DEPT_NBR = as.numeric(Base_cann$AGC_DEPT_NBR)
setkey(Base_cann, AGC_DEPT_NBR, ANTI_GC)

#Base Halo Basket Data
query <- paste("SELECT * FROM dbcure.PROMOX_HALO_LEVELID_SCORECARD_auto WHERE FOCUS_DEPT_NBR IN (",paste(shQuote(Depts, type="sh"), collapse=", "),")")
#Base_halo <- dataset.load(name = 'GCP DD', query = query)
Base_halo <- contry('load',10,5,query)
Base_halo <- as.data.table(Base_halo)
Base_halo <- unique(Base_halo)

names(Base_halo) <- toupper(names(Base_halo))
Base_halo$FOCUS_GC = as.character(Base_halo$FOCUS_GC)
Base_halo$AFFINED_GC = as.character(Base_halo$AFFINED_GC)
Base_halo$FOCUS_DEPT_NBR = as.numeric(Base_halo$FOCUS_DEPT_NBR)
Base_halo$AFFINED_DEPT_NBR = as.numeric(Base_halo$AFFINED_DEPT_NBR)
setkey(Base_halo, AFFINED_DEPT_NBR, AFFINED_GC)

#Base PF data
query <- paste("SELECT * FROM dbcure.PROMOX_PF_CORR_SCORECARD_auto WHERE FGC_DEPT_NBR IN (",paste(shQuote(Depts, type="sh"), collapse=", "),")")
#Base_PF <- dataset.load(name = 'GCP DD', query = query)
Base_PF <- contry('load',10,5,query)
Base_PF <- as.data.table(Base_PF)
Base_PF <- unique(Base_PF)

names(Base_PF) <- toupper(names(Base_PF))
Base_PF$FOCUS_GC = as.character(Base_PF$FOCUS_GC)
Base_PF$FGC_DEPT_NBR = as.numeric(Base_PF$FGC_DEPT_NBR)
setkey(Base_PF, FGC_DEPT_NBR)

#Forecast data

#GCs on promo from forecast table
query <- paste("SELECT DISTINCT GROUP_CODE, DEPT_NBR FROM dbcure.PROMOX_MODEL_FORECAST_DATA WHERE FLYER_FLAG = 1 AND WM_YR_WK BETWEEN ",week_forecast_start," AND ",week_forecast_end," AND DEPT_NBR IN (",paste(Depts,collapse=", "),") ")
#Base_forecast_GC <- dataset.load(name = 'GCP DD', query = query)
Base_forecast_GC <- contry('load',10,5,query)
Base_forecast_GC <- as.data.table(Base_forecast_GC)

names(Base_forecast_GC) <- toupper(names(Base_forecast_GC))
Base_forecast_GC$GROUP_CODE = as.character(Base_forecast_GC$GROUP_CODE)
Base_forecast_GC$DEPT_NBR = as.numeric(Base_forecast_GC$DEPT_NBR)
setkey(Base_forecast_GC, DEPT_NBR)



#Time_dp_begin = Sys.time()

##Data Load 
for(x in 1:length(Depts))
{
  
  Dept_Nbr = Depts[x]
  
  ##Data Load 
  
  ##Base Dept Data
  Base_dept_data <- Base_data[J(Dept_Nbr) ,nomatch = 0L]
  if(nrow(Base_dept_data)[1] == 0){next}
  setkey(Base_dept_data, GROUP_CODE, WM_YR_WK)
  
  #Base Dept PF
  Base_dept_PF <- Base_PF[J(Dept_Nbr) ,nomatch = 0L]
  if(nrow(Base_dept_PF)[1] == 0){next}
  setkey(Base_dept_PF, FOCUS_GC)
  
  #Extracting the list of group codes that are on promotion in the week
  Focus_GC_list <- unique(Base_dept_data[rowSums(Base_dept_data[,c('FLYER_FLAG','CPP_FLAG','DIGEST_FLAG','DIGITAL_FLAG')]) > 0 & WM_YR_WK == week_end]$GROUP_CODE)
  
  Base_forecast_dept_GC <- Base_forecast_GC[J(Dept_Nbr) ,nomatch = 0L]
  
  #Extracting the list of FC group codes that are on promotion in 12 weeks
  FC_Focus_GC_list <- unique(Base_forecast_dept_GC$GROUP_CODE)
  
  ##Base Dept Cannibalization Data
  Base_cann_data <- Base_cann[J(Dept_Nbr) ,nomatch = 0L]
  setkey(Base_cann_data, ANTI_GC, FOCUS_GC) #anti,focus
  Cann_GC_list <- unique(Base_cann_data$ANTI_GC)
  
  ##Base Dept Halo Basket Data
  Base_halo_data <- Base_halo[J(Dept_Nbr) ,nomatch = 0L]
  setkey(Base_halo_data, AFFINED_GC, FOCUS_GC) #affined,focus
  Halo_GC_list <- unique(Base_halo_data$AFFINED_GC)
  
  temp_Base_halo <- Base_halo_data %>% distinct(FOCUS_GC, FOCUS_DEPT_NBR, .keep_all = TRUE) 
  setkey(temp_Base_halo, FOCUS_GC)
  
  
  ##End of Data Load
  
  ##Data Initalization
  
  #Unioning all GroupCodes
  GroupCode <- unique(c(Focus_GC_list, FC_Focus_GC_list, Halo_GC_list, Cann_GC_list))
  GroupCode = sort(GroupCode)
  
  
  if(length(GroupCode)==0){next}
  
  data_not_generated = numeric()
  final_insights_allgc = data.frame()
  
  #Add Log file
  main_log <- data.frame(matrix(ncol = 19))
  colnames(main_log) = c("Group_Code","Region","Volume_status","Sparsity_status",
                         "REG_PRICE_status","Trend_status","RandomForest_Iterations",
                         "RandomForestruns","OLS_status","Number_of_regions","Adjusted_rsquared",
                         "MAPE_sku","F_statistic","p_value_of_F_statistic","RMSE","RSquared",
                         "Trend_RMSE", "Forecast_WMAPE", "Forecast_MAPE")
  
  #Add Insights
  Insights <- data.frame(matrix(ncol = 15))
  colnames(Insights) <- c("GROUP_CODE","WM_REGION",
                          "PA_WEIGHTED_REG","AR_WEIGHTED_REG","TR_WEIGHTED_REG",
                          "PAGE_LOCATIONBACK","PAGE_LOCATIONFRONT","PAGE_LOCATIONINSIDE",
                          "CPP_FLAG","DIGITAL_FLAG","DIGEST_FLAG",
                          "Positive_Significant","Negative_Significant","Positive_Insignificant","Negative_Insignificant")
  
  #Add net main columns, note we need to change this
  Main_File <- data.frame(matrix(ncol = 167))
  colnames(Main_File) <- c( "WM_YR_WK", "GROUP_CODE", "QTY", "MODELLED_QTY", "COMPASS_BASELINE_QTY", 
                            "COMPASS_MODELLED_QTY", "COMPASS_TOTAL_QTY", "COMPASS_BASELINE_SALES", "COMPASS_TOTAL_SALES",                           
                            "COMPASS_SALES_PROMOLIFT", "COMPASS_SALES_PROMOLIFT_PERCENT", "trend_var",                                    
                            "REG_PRICE", "PA_WEIGHTED_REG_COEF", "AR_WEIGHTED_REG_COEF", "TR_WEIGHTED_REG_COEF",                        
                            "PAGE_LOCATIONBACK_COEF", "PAGE_LOCATIONFRONT_COEF", "PAGE_LOCATIONINSIDE_COEF", "CPP_FLAG_COEF",                                 
                            "DIGITAL_FLAG_COEF", "DIGEST_FLAG_COEF", 
                            "week.1","week.2","week.3","week.4","week.5","week.6","week.7","week.8","week.9","week.10",
                            "week.11","week.12","week.13","week.14","week.15","week.16","week.17","week.18","week.19","week.20",
                            "week.21","week.22","week.23","week.24","week.25","week.26","week.27","week.28","week.29","week.30",
                            "week.31","week.32","week.33","week.34","week.35","week.36","week.37","week.38","week.39","week.40",
                            "week.41","week.42","week.43","week.44","week.45","week.46","week.47","week.48","week.49","week.50",
                            "week.51","week.52", 
                            "CANADA_DAY_COEF", "COMMONWEALTH_DAY_COEF", "CHRISTMAS_DAY_COEF", "THANKSGIVING_DAY_COEF",                         
                            "GROUNDHOG_DAY_COEF", "LABOR_DAY_COEF", "VALENTINES_DAY_COEF", "NATIONAL_ABORGINAL_DAY_COEF",                   
                            "ST_PATRICKS_DAY_COEF", "EPIPHANY_DAY_COEF", "NATIONAL_TARTAN_DAY_COEF", "REMEMBRANCE_DAY_COEF",                          
                            "HLTHCR_AD_DAY_COEF", "VIMY_RIDGE_DAY_COEF", "GOOD_FRIDAY_COEF", "NEW_YEARS_COEF",                               
                            "ANNIVERSARY_OF_THE_STATUE_OF_WESTMINISTER_COEF", "HALLOWEEN_COEF", 
                            "PA_WEIGHTED_REG_lag_1_COEF", "AR_WEIGHTED_REG_lag_1_COEF", "TR_WEIGHTED_REG_lag_1_COEF",      
                            "DIGEST_FLAG_lag_1_COEF", "DIGITAL_FLAG_lag_1_COEF", 
                            "CPP_FLAG_lag_1_COEF", "PAGE_LOCATIONINSIDE_lag_1_COEF", "PAGE_LOCATIONFRONT_lag_1_COEF"  , 
                            "PAGE_LOCATIONBACK_lag_1_COEF", "PA_WEIGHTED_REG_lag_2_COEF", "AR_WEIGHTED_REG_lag_2_COEF",
                            "TR_WEIGHTED_REG_lag_2_COEF", "DIGEST_FLAG_lag_2_COEF", "DIGITAL_FLAG_lag_2_COEF", 
                            "CPP_FLAG_lag_2_COEF", "PAGE_LOCATIONINSIDE_lag_2_COEF", "PAGE_LOCATIONFRONT_lag_2_COEF", 
                            "PAGE_LOCATIONBACK_lag_2_COEF", "PA_WEIGHTED_REG_lag_3_COEF", "AR_WEIGHTED_REG_lag_3_COEF", 
                            "TR_WEIGHTED_REG_lag_3_COEF","DIGEST_FLAG_lag_3_COEF", "DIGITAL_FLAG_lag_3_COEF",         
                            "CPP_FLAG_lag_3_COEF" ,"PAGE_LOCATIONINSIDE_lag_3_COEF", "PAGE_LOCATIONFRONT_lag_3_COEF", 
                            "PAGE_LOCATIONBACK_lag_3_COEF","PA_WEIGHTED_REG_lag_4_COEF", "AR_WEIGHTED_REG_lag_4_COEF", 
                            "TR_WEIGHTED_REG_lag_4_COEF", "DIGEST_FLAG_lag_4_COEF", "DIGITAL_FLAG_lag_4_COEF", 
                            "CPP_FLAG_lag_4_COEF", "PAGE_LOCATIONINSIDE_lag_4_COEF", "PAGE_LOCATIONFRONT_lag_4_COEF", 
                            "PAGE_LOCATIONBACK_lag_4_COEF", "PA_WEIGHTED_REG_lag_1_CONTRI", "AR_WEIGHTED_REG_lag_1_CONTRI", 
                            "TR_WEIGHTED_REG_lag_1_CONTRI", "DIGEST_FLAG_lag_1_CONTRI", "DIGITAL_FLAG_lag_1_CONTRI", 
                            "CPP_FLAG_lag_1_CONTRI", "PAGE_LOCATIONINSIDE_lag_1_CONTRI" ,"PAGE_LOCATIONFRONT_lag_1_CONTRI", 
                            "PAGE_LOCATIONBACK_lag_1_CONTRI", "PA_WEIGHTED_REG_lag_2_CONTRI", "AR_WEIGHTED_REG_lag_2_CONTRI", 
                            "TR_WEIGHTED_REG_lag_2_CONTRI", "DIGEST_FLAG_lag_2_CONTRI", "DIGITAL_FLAG_lag_2_CONTRI", 
                            "CPP_FLAG_lag_2_CONTRI", "PAGE_LOCATIONINSIDE_lag_2_CONTRI", "PAGE_LOCATIONFRONT_lag_2_CONTRI", 
                            "PAGE_LOCATIONBACK_lag_2_CONTRI", "PA_WEIGHTED_REG_lag_3_CONTRI", "AR_WEIGHTED_REG_lag_3_CONTRI", 
                            "TR_WEIGHTED_REG_lag_3_CONTRI", "DIGEST_FLAG_lag_3_CONTRI", "DIGITAL_FLAG_lag_3_CONTRI", 
                            "CPP_FLAG_lag_3_CONTRI", "PAGE_LOCATIONINSIDE_lag_3_CONTRI" ,"PAGE_LOCATIONFRONT_lag_3_CONTRI", 
                            "PAGE_LOCATIONBACK_lag_3_CONTRI", "PA_WEIGHTED_REG_lag_4_CONTRI", "AR_WEIGHTED_REG_lag_4_CONTRI", 
                            "TR_WEIGHTED_REG_lag_4_CONTRI", "DIGEST_FLAG_lag_4_CONTRI", "DIGITAL_FLAG_lag_4_CONTRI", 
                            "CPP_FLAG_lag_4_CONTRI", "PAGE_LOCATIONINSIDE_lag_4_CONTRI" ,"PAGE_LOCATIONFRONT_lag_4_CONTRI", 
                            "PAGE_LOCATIONBACK_lag_4_CONTRI",
                            "INTERCEPT", "WM_REGION", "FITTED_VALUES")
  
  Cann_halo_main_File <- data.frame(matrix(ncol = 12))
  colnames(Cann_halo_main_File) <- c("PA_WEIGHTED_REG_FOCUS_COEF", "AR_WEIGHTED_REG_FOCUS_COEF","TR_WEIGHTED_REG_FOCUS_COEF", 
                                     "PAGE_LOCATIONFRONT_FOCUS_COEF", "PAGE_LOCATIONINSIDE_FOCUS_COEF", "PAGE_LOCATIONBACK_FOCUS_COEF", 
                                     "CPP_FLAG_FOCUS_COEF", "DIGITAL_FLAG_FOCUS_COEF", "DIGEST_FLAG_FOCUS_COEF",
                                     "GROUP_CODE", "FOCUS_GC","FOCUS_GC_DEPT")
  
  
  
  #######Modelling the data
  
  #Batchwise mclapply
  out_batchsize = 200
  batchsize <- 75
  
  for(jj in seq(1, length(GroupCode), out_batchsize))
  {
    if(c(length(GroupCode) - jj) < out_batchsize)
    {
      ###Data prepared as a list of data frames
      loop_again = TRUE
      
      while(loop_again)
      {
        loop_again = FALSE
        gc()
        
        tryCatch(
          {
            R.utils::withTimeout({
              Data_frame_list_prep = mclapply(c(jj:length(GroupCode)), Data_prep , mc.cores = 28, mc.cleanup = TRUE)
              gc()
              
            }, timeout = 15000, elapsed = 15000, cpu = Inf)
          }, TimeoutException = function(ex){loop_again <<- TRUE}
        )
      }
      
      if(length(Data_frame_list_prep) == 0){next}
      
      print('Data Prep complete')
      
      for(j in seq(1,length(Data_frame_list_prep), batchsize))
      {
        loop_again = TRUE
        
        while(loop_again)
        {
          loop_again = FALSE
          gc()
          
          tryCatch(
            {
              R.utils::withTimeout({
                if(c(length(Data_frame_list_prep) - j) < batchsize)
                {
                  rm(list=setdiff(ls(), c("Depts",
                                          "rolling_weeks","Forecast_length","week_end","week_start","week_forecast_end","week_forecast_start",
                                          "calendar","week_cnt",
                                          "Promotions","Promos",
                                          "Base_data","Base_data_GC","Base_cann","Base_halo","Base_PF","Base_forecast_GC",
                                          "Dept_Nbr",
                                          "Base_dept_data","Base_dept_PF", "Focus_GC_list", "Base_forecast_dept_GC", "Base_cann_data", "Cann_GC_list",
                                          "Base_halo_data", "Halo_GC_list", "temp_Base_halo", "GroupCode", 
                                          "data_not_generated", "final_insights_allgc",
                                          "main_log", "Insights", "Main_File", "Cann_halo_main_File", 
                                          "out_batchsize", "batchsize", 
                                          "Data_frame_list_prep", "jj", "j", "loop_again",
                                          ls.str(mode="function"))))
                  
                  out_results <- mclapply(c(j:length(Data_frame_list_prep)), Region_loop, mc.cores = 25, mc.cleanup = TRUE) 
                  Final_list <- purrr::transpose(out_results) %>% map(dplyr::bind_rows)
                  gc()
                  print('Data Model complete')
                  
                  Insights_file_parallel = Final_list[[1]]
                  Log_file_parallel = Final_list[[2]]
                  Baseline_results_parallel = Final_list[[3]]
                  Halo_results_parallel = Final_list[[4]]
                  Cann_results_parallel = Final_list[[5]]
                  
                  #Cutting out values that we do not need 
                  #Baseline_results_parallel <- Baseline_results_parallel[Baseline_results_parallel$GROUP_CODE %in% Focus_GC_list,]
                  Baseline_results_parallel <- Baseline_results_parallel[Baseline_results_parallel$GROUP_CODE %in% unique(c(Focus_GC_list, Halo_GC_list, Cann_GC_list)) ,]
                  Halo_results_parallel <- Halo_results_parallel[Halo_results_parallel$GROUP_CODE %in% Halo_GC_list,]
                  Cann_results_parallel <- Cann_results_parallel[Cann_results_parallel$GROUP_CODE %in% Cann_GC_list,]
                  
                  setcolorder(Baseline_results_parallel, colnames(Main_File))
                  
                  Log_file_parallel$AGC_Dept = Dept_Nbr
                  Log_file_parallel$SCORECARD_WK = week_end
                  if(dim(Baseline_results_parallel)[1]!=0)
                  {
                    Insights_file_parallel$AGC_Dept = Dept_Nbr
                    Insights_file_parallel$SCORECARD_WK = week_end
                    Baseline_results_parallel$AGC_Dept = Dept_Nbr
                    Baseline_results_parallel$SCORECARD_WK <- week_end
                  }
                  
                  if(dim(Halo_results_parallel)[1]!=0)
                  {
                    Halo_results_parallel$AGC_Dept = Dept_Nbr
                    Halo_results_parallel$SCORECARD_WK = week_end
                  }
                  
                  if(dim(Cann_results_parallel)[1]!=0)
                  {
                    Cann_results_parallel$AGC_Dept = Dept_Nbr
                    Cann_results_parallel$SCORECARD_WK = week_end
                  }
                  
                  
                  #Workflow + hdfs, change this in data_clean function
                  #replacing "." with "_" in column names
                  colnames(Baseline_results_parallel) <- gsub(x = colnames(Baseline_results_parallel), pattern = "\\.", replacement = "_")
                  colnames(Halo_results_parallel) <- gsub(x = colnames(Halo_results_parallel), pattern = "\\.", replacement = "_")
                  colnames(Cann_results_parallel) <- gsub(x = colnames(Cann_results_parallel), pattern = "\\.", replacement = "_")
                  colnames(Log_file_parallel) <- gsub(x = colnames(Log_file_parallel), pattern = "\\.", replacement = "_")
                  colnames(Insights_file_parallel) <- gsub(x = colnames(Insights_file_parallel), pattern = "\\.", replacement = "_")
                  
                  res_strng <- paste("/data/GCP/Aggregated_Model/Results/", as.character(Dept_Nbr), "_Results_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  log_strng <- paste("/data/GCP/Aggregated_Model/Logs/", as.character(Dept_Nbr), "_Log_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  ins_strng <- paste("/data/GCP/Aggregated_Model/Insights/", as.character(Dept_Nbr), "_Insights_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  halo_strng <- paste("/data/GCP/Aggregated_Model/Halo/", as.character(Dept_Nbr), "_Halo_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  cann_strng <- paste("/data/GCP/Aggregated_Model/Cann/", as.character(Dept_Nbr), "_Cann_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  gc()
                  
                  #saving dfs to hdfs csvs
                  if(nrow(Baseline_results_parallel) > 0)
                  {
                    write.csv(Baseline_results_parallel, paste0(res_strng), row.names = FALSE)
                  }
                  
                  if(nrow(Log_file_parallel) > 0)
                  {
                    write.csv(Log_file_parallel, paste0(log_strng), row.names = FALSE)
                  }
                  
                  if(nrow(Insights_file_parallel) > 0)
                  {
                    write.csv(Insights_file_parallel, paste0(ins_strng), row.names = FALSE)
                  }
                  
                  if(nrow(Halo_results_parallel) > 0)
                  {
                    write.csv(Halo_results_parallel, paste0(halo_strng), row.names = FALSE)
                  }else
                  {
                    write.csv(Cann_halo_main_File, paste0(halo_strng), row.names = FALSE)
                  }
                  
                  if(nrow(Cann_results_parallel) > 0)
                  {
                    write.csv(Cann_results_parallel, paste0(cann_strng), row.names = FALSE)
                  }else
                  {
                    write.csv(Cann_halo_main_File, paste0(cann_strng), row.names = FALSE)
                  }
                  
                  gc()
                  
                  break
                }
                
                rm(list=setdiff(ls(), c("Depts",
                                        "rolling_weeks","Forecast_length","week_end","week_start","week_forecast_end","week_forecast_start",
                                        "calendar","week_cnt",
                                        "Promotions","Promos",
                                        "Base_data","Base_data_GC","Base_cann","Base_halo","Base_PF","Base_forecast_GC",
                                        "Dept_Nbr",
                                        "Base_dept_data","Base_dept_PF", "Focus_GC_list", "Base_forecast_dept_GC", "Base_cann_data", "Cann_GC_list",
                                        "Base_halo_data", "Halo_GC_list", "temp_Base_halo", "GroupCode", 
                                        "data_not_generated", "final_insights_allgc",
                                        "main_log", "Insights", "Main_File", "Cann_halo_main_File", 
                                        "out_batchsize", "batchsize", 
                                        "Data_frame_list_prep", "jj", "j", "loop_again",
                                        ls.str(mode="function"))))
                
                out_results <- mclapply(c(j:c(j+batchsize-1)), Region_loop, mc.cores = 25, mc.cleanup = TRUE)
                Final_list <- purrr::transpose(out_results) %>% map(dplyr::bind_rows)
                gc()
                
                print('Data Model complete')
                
                
                Insights_file_parallel = Final_list[[1]]
                Log_file_parallel = Final_list[[2]]
                Baseline_results_parallel = Final_list[[3]]
                Halo_results_parallel = Final_list[[4]]
                Cann_results_parallel = Final_list[[5]]
                
                #Cleaning the frames based upon the group code type they have
                #Baseline_results_parallel <- Baseline_results_parallel[Baseline_results_parallel$GROUP_CODE %in% Focus_GC_list,]
                Baseline_results_parallel <- Baseline_results_parallel[Baseline_results_parallel$GROUP_CODE %in% unique(c(Focus_GC_list, Halo_GC_list, Cann_GC_list)) ,]
                Halo_results_parallel <- Halo_results_parallel[Halo_results_parallel$GROUP_CODE %in% Halo_GC_list,]
                Cann_results_parallel <- Cann_results_parallel[Cann_results_parallel$GROUP_CODE %in% Cann_GC_list,]
                
                setcolorder(Baseline_results_parallel, colnames(Main_File))
                
                Log_file_parallel$AGC_Dept = Dept_Nbr
                Log_file_parallel$SCORECARD_WK = week_end
                if(dim(Baseline_results_parallel)[1]!=0)
                {
                  Insights_file_parallel$AGC_Dept = Dept_Nbr
                  Insights_file_parallel$SCORECARD_WK = week_end
                  Baseline_results_parallel$AGC_Dept = Dept_Nbr
                  Baseline_results_parallel$SCORECARD_WK <- week_end
                }
                
                if(dim(Halo_results_parallel)[1]!=0)
                {
                  Halo_results_parallel$AGC_Dept = Dept_Nbr
                  Halo_results_parallel$SCORECARD_WK = week_end
                }
                
                if(dim(Cann_results_parallel)[1]!=0)
                {
                  Cann_results_parallel$AGC_Dept = Dept_Nbr
                  Cann_results_parallel$SCORECARD_WK = week_end
                }
                
                
                #Workflow + hdfs, change this in data_clean function
                #replacing "." with "_" in column names
                colnames(Baseline_results_parallel) <- gsub(x = colnames(Baseline_results_parallel), pattern = "\\.", replacement = "_")
                colnames(Halo_results_parallel) <- gsub(x = colnames(Halo_results_parallel), pattern = "\\.", replacement = "_")
                colnames(Cann_results_parallel) <- gsub(x = colnames(Cann_results_parallel), pattern = "\\.", replacement = "_")
                colnames(Log_file_parallel) <- gsub(x = colnames(Log_file_parallel), pattern = "\\.", replacement = "_")
                colnames(Insights_file_parallel) <- gsub(x = colnames(Insights_file_parallel), pattern = "\\.", replacement = "_")
                
                res_strng <- paste("/data/GCP/Aggregated_Model/Results/", as.character(Dept_Nbr), "_Results_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                log_strng <- paste("/data/GCP/Aggregated_Model/Logs/", as.character(Dept_Nbr), "_Log_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                ins_strng <- paste("/data/GCP/Aggregated_Model/Insights/", as.character(Dept_Nbr), "_Insights_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                halo_strng <- paste("/data/GCP/Aggregated_Model/Halo/", as.character(Dept_Nbr), "_Halo_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                cann_strng <- paste("/data/GCP/Aggregated_Model/Cann/", as.character(Dept_Nbr), "_Cann_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                gc()
                
                #saving dfs to hdfs csvs
                if(nrow(Baseline_results_parallel) > 0)
                {
                  write.csv(Baseline_results_parallel, paste0(res_strng), row.names = FALSE)
                }
                
                if(nrow(Log_file_parallel) > 0)
                {
                  write.csv(Log_file_parallel, paste0(log_strng), row.names = FALSE)
                }
                
                if(nrow(Insights_file_parallel) > 0)
                {
                  write.csv(Insights_file_parallel, paste0(ins_strng), row.names = FALSE)
                }
                
                if(nrow(Halo_results_parallel) > 0)
                {
                  write.csv(Halo_results_parallel, paste0(halo_strng), row.names = FALSE)
                }else
                {
                  write.csv(Cann_halo_main_File, paste0(halo_strng), row.names = FALSE)
                }
                
                if(nrow(Cann_results_parallel) > 0)
                {
                  write.csv(Cann_results_parallel, paste0(cann_strng), row.names = FALSE)
                }else
                {
                  write.csv(Cann_halo_main_File, paste0(cann_strng), row.names = FALSE)
                }
                
                gc()
                
              }, timeout = 15000, elapsed = 15000, cpu = Inf)
            }, TimeoutException = function(ex){loop_again <<- TRUE}
          )
          
        }
      }
    }else
    {      
      ###Data prepared as a list of data frames
      loop_again = TRUE
      
      while(loop_again)
      {
        loop_again = FALSE
        gc()
        
        tryCatch(
          {
            R.utils::withTimeout({
              Data_frame_list_prep <- mclapply(c(jj : (jj + out_batchsize - 1)), Data_prep, mc.cores = 28, mc.cleanup = TRUE)
              gc()
              
            }, timeout = 15000, elapsed = 15000, cpu = Inf)
          }, TimeoutException = function(ex){loop_again <<- TRUE}
        )
      }
      
      print('Data Prep complete')
      
      for(j in seq(1,length(Data_frame_list_prep), batchsize))
      {
        loop_again = TRUE
        
        while(loop_again)
        {
          loop_again = FALSE
          gc()
          
          tryCatch(
            {
              R.utils::withTimeout({
                if(c(length(Data_frame_list_prep) - j) < batchsize)
                {
                  rm(list=setdiff(ls(), c("Depts",
                                          "rolling_weeks","Forecast_length","week_end","week_start","week_forecast_end","week_forecast_start",
                                          "calendar","week_cnt",
                                          "Promotions","Promos",
                                          "Base_data","Base_data_GC","Base_cann","Base_halo","Base_PF","Base_forecast_GC",
                                          "Dept_Nbr",
                                          "Base_dept_data","Base_dept_PF", "Focus_GC_list", "Base_forecast_dept_GC", "Base_cann_data", "Cann_GC_list",
                                          "Base_halo_data", "Halo_GC_list", "temp_Base_halo", "GroupCode", 
                                          "data_not_generated", "final_insights_allgc",
                                          "main_log", "Insights", "Main_File", "Cann_halo_main_File", 
                                          "out_batchsize", "batchsize", 
                                          "Data_frame_list_prep", "jj", "j", "loop_again",
                                          ls.str(mode="function"))))
                  
                  out_results <- mclapply(c(j:length(Data_frame_list_prep)), Region_loop, mc.cores = 25, mc.cleanup = TRUE) 
                  Final_list <- purrr::transpose(out_results) %>% map(dplyr::bind_rows)
                  gc()
                  print('Data Model complete')
                  
                  Insights_file_parallel = Final_list[[1]]
                  Log_file_parallel = Final_list[[2]]
                  Baseline_results_parallel = Final_list[[3]]
                  Halo_results_parallel = Final_list[[4]]
                  Cann_results_parallel = Final_list[[5]]
                  
                  #Cutting out values that we do not need 
                  #Baseline_results_parallel <- Baseline_results_parallel[Baseline_results_parallel$GROUP_CODE %in% Focus_GC_list,]
                  Baseline_results_parallel <- Baseline_results_parallel[Baseline_results_parallel$GROUP_CODE %in% unique(c(Focus_GC_list, Halo_GC_list, Cann_GC_list)) ,]
                  Halo_results_parallel <- Halo_results_parallel[Halo_results_parallel$GROUP_CODE %in% Halo_GC_list,]
                  Cann_results_parallel <- Cann_results_parallel[Cann_results_parallel$GROUP_CODE %in% Cann_GC_list,]
                  
                  setcolorder(Baseline_results_parallel, colnames(Main_File))
                  
                  Log_file_parallel$AGC_Dept = Dept_Nbr
                  Log_file_parallel$SCORECARD_WK = week_end
                  if(dim(Baseline_results_parallel)[1]!=0)
                  {
                    Insights_file_parallel$AGC_Dept = Dept_Nbr
                    Insights_file_parallel$SCORECARD_WK = week_end
                    Baseline_results_parallel$AGC_Dept = Dept_Nbr
                    Baseline_results_parallel$SCORECARD_WK <- week_end
                  }
                  
                  if(dim(Halo_results_parallel)[1]!=0)
                  {
                    Halo_results_parallel$AGC_Dept = Dept_Nbr
                    Halo_results_parallel$SCORECARD_WK = week_end
                  }
                  
                  if(dim(Cann_results_parallel)[1]!=0)
                  {
                    Cann_results_parallel$AGC_Dept = Dept_Nbr
                    Cann_results_parallel$SCORECARD_WK = week_end
                  }
                  
                  
                  #Workflow + hdfs, change this in data_clean function
                  #replacing "." with "_" in column names
                  colnames(Baseline_results_parallel) <- gsub(x = colnames(Baseline_results_parallel), pattern = "\\.", replacement = "_")
                  colnames(Halo_results_parallel) <- gsub(x = colnames(Halo_results_parallel), pattern = "\\.", replacement = "_")
                  colnames(Cann_results_parallel) <- gsub(x = colnames(Cann_results_parallel), pattern = "\\.", replacement = "_")
                  colnames(Log_file_parallel) <- gsub(x = colnames(Log_file_parallel), pattern = "\\.", replacement = "_")
                  colnames(Insights_file_parallel) <- gsub(x = colnames(Insights_file_parallel), pattern = "\\.", replacement = "_")
                  
                  res_strng <- paste("/data/GCP/Aggregated_Model/Results/", as.character(Dept_Nbr), "_Results_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  log_strng <- paste("/data/GCP/Aggregated_Model/Logs/", as.character(Dept_Nbr), "_Log_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  ins_strng <- paste("/data/GCP/Aggregated_Model/Insights/", as.character(Dept_Nbr), "_Insights_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  halo_strng <- paste("/data/GCP/Aggregated_Model/Halo/", as.character(Dept_Nbr), "_Halo_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  cann_strng <- paste("/data/GCP/Aggregated_Model/Cann/", as.character(Dept_Nbr), "_Cann_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                  gc()
                  
                  #saving dfs to hdfs csvs
                  if(nrow(Baseline_results_parallel) > 0)
                  {
                    write.csv(Baseline_results_parallel, paste0(res_strng), row.names = FALSE)
                  }
                  
                  if(nrow(Log_file_parallel) > 0)
                  {
                    write.csv(Log_file_parallel, paste0(log_strng), row.names = FALSE)
                  }
                  
                  if(nrow(Insights_file_parallel) > 0)
                  {
                    write.csv(Insights_file_parallel, paste0(ins_strng), row.names = FALSE)
                  }
                  
                  if(nrow(Halo_results_parallel) > 0)
                  {
                    write.csv(Halo_results_parallel, paste0(halo_strng), row.names = FALSE)
                  }else
                  {
                    write.csv(Cann_halo_main_File, paste0(halo_strng), row.names = FALSE)
                  }
                  
                  if(nrow(Cann_results_parallel) > 0)
                  {
                    write.csv(Cann_results_parallel, paste0(cann_strng), row.names = FALSE)
                  }else
                  {
                    write.csv(Cann_halo_main_File, paste0(cann_strng), row.names = FALSE)
                  }
                  
                  gc()
                  
                  break
                }
                
                rm(list=setdiff(ls(), c("Depts",
                                        "rolling_weeks","Forecast_length","week_end","week_start","week_forecast_end","week_forecast_start",
                                        "calendar","week_cnt",
                                        "Promotions","Promos",
                                        "Base_data","Base_data_GC","Base_cann","Base_halo","Base_PF","Base_forecast_GC",
                                        "Dept_Nbr",
                                        "Base_dept_data","Base_dept_PF", "Focus_GC_list", "Base_forecast_dept_GC", "Base_cann_data", "Cann_GC_list",
                                        "Base_halo_data", "Halo_GC_list", "temp_Base_halo", "GroupCode", 
                                        "data_not_generated", "final_insights_allgc",
                                        "main_log", "Insights", "Main_File", "Cann_halo_main_File", 
                                        "out_batchsize", "batchsize", 
                                        "Data_frame_list_prep", "jj", "j", "loop_again",
                                        ls.str(mode="function"))))
                
                out_results <- mclapply(c(j:c(j + batchsize - 1)), Region_loop, mc.cores = 25, mc.cleanup = TRUE)
                Final_list <- purrr::transpose(out_results) %>% map(dplyr::bind_rows)
                gc()
                
                Insights_file_parallel = Final_list[[1]]
                Log_file_parallel = Final_list[[2]]
                Baseline_results_parallel = Final_list[[3]]
                Halo_results_parallel = Final_list[[4]]
                Cann_results_parallel = Final_list[[5]]
                
                #Cleaning the frames based upon the group code type they have
                #Baseline_results_parallel <- Baseline_results_parallel[Baseline_results_parallel$GROUP_CODE %in% Focus_GC_list,]
                Baseline_results_parallel <- Baseline_results_parallel[Baseline_results_parallel$GROUP_CODE %in% unique(c(Focus_GC_list, Halo_GC_list, Cann_GC_list)) ,]
                Halo_results_parallel <- Halo_results_parallel[Halo_results_parallel$GROUP_CODE %in% Halo_GC_list,]
                Cann_results_parallel <- Cann_results_parallel[Cann_results_parallel$GROUP_CODE %in% Cann_GC_list,]
                
                setcolorder(Baseline_results_parallel, colnames(Main_File))
                
                Log_file_parallel$AGC_Dept = Dept_Nbr
                Log_file_parallel$SCORECARD_WK = week_end
                if(dim(Baseline_results_parallel)[1]!=0)
                {
                  Insights_file_parallel$AGC_Dept = Dept_Nbr
                  Insights_file_parallel$SCORECARD_WK = week_end
                  Baseline_results_parallel$AGC_Dept = Dept_Nbr
                  Baseline_results_parallel$SCORECARD_WK <- week_end
                }
                
                if(dim(Halo_results_parallel)[1]!=0)
                {
                  Halo_results_parallel$AGC_Dept = Dept_Nbr
                  Halo_results_parallel$SCORECARD_WK = week_end
                }
                
                if(dim(Cann_results_parallel)[1]!=0)
                {
                  Cann_results_parallel$AGC_Dept = Dept_Nbr
                  Cann_results_parallel$SCORECARD_WK = week_end
                }
                
                
                #Workflow + hdfs, change this in data_clean function
                #replacing "." with "_" in column names
                colnames(Baseline_results_parallel) <- gsub(x = colnames(Baseline_results_parallel), pattern = "\\.", replacement = "_")
                colnames(Halo_results_parallel) <- gsub(x = colnames(Halo_results_parallel), pattern = "\\.", replacement = "_")
                colnames(Cann_results_parallel) <- gsub(x = colnames(Cann_results_parallel), pattern = "\\.", replacement = "_")
                colnames(Log_file_parallel) <- gsub(x = colnames(Log_file_parallel), pattern = "\\.", replacement = "_")
                colnames(Insights_file_parallel) <- gsub(x = colnames(Insights_file_parallel), pattern = "\\.", replacement = "_")
                
                res_strng <- paste("/data/GCP/Aggregated_Model/Results/", as.character(Dept_Nbr), "_Results_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                log_strng <- paste("/data/GCP/Aggregated_Model/Logs/", as.character(Dept_Nbr), "_Log_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                ins_strng <- paste("/data/GCP/Aggregated_Model/Insights/", as.character(Dept_Nbr), "_Insights_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                halo_strng <- paste("/data/GCP/Aggregated_Model/Halo/", as.character(Dept_Nbr), "_Halo_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                cann_strng <- paste("/data/GCP/Aggregated_Model/Cann/", as.character(Dept_Nbr), "_Cann_",jj,"_",j,"_",as.character(Sys.Date()), ".csv", sep="")
                gc()
                
                #saving dfs to elements local csvs
                if(nrow(Baseline_results_parallel) > 0)
                {
                  write.csv(Baseline_results_parallel,  paste0(res_strng), row.names = FALSE)
                }
                
                if(nrow(Log_file_parallel) > 0)
                {
                  write.csv(Log_file_parallel, paste0(log_strng), row.names = FALSE)
                }
                
                if(nrow(Insights_file_parallel) > 0)
                {
                  write.csv(Insights_file_parallel, paste0(ins_strng), row.names = FALSE)
                }
                
                if(nrow(Halo_results_parallel) > 0)
                {
                  write.csv(Halo_results_parallel, paste0(halo_strng), row.names = FALSE)
                }else
                {
                  write.csv(Cann_halo_main_File,  paste0(halo_strng), row.names = FALSE)
                }
                
                if(nrow(Cann_results_parallel) > 0)
                {
                  write.csv(Cann_results_parallel, paste0(cann_strng),row.names = FALSE)
                }else
                {
                  write.csv(Cann_halo_main_File, paste0(cann_strng),row.names = FALSE)
                }
                
                gc()
                
              }, timeout = 14000, elapsed = 14000, cpu = Inf)
            }, TimeoutException = function(ex){loop_again <<- TRUE}
          )
          
        }
      }
      
      
    }
  }
  
  
  
}
connector.upload(name="elements_to_gcs_prod", container="b76c14154ee4d5d351c531a0afacb1647570f3a14ab1de74", source_path="/data/GCP/Aggregated_Model/Results/", target_path="dbcure/Aggregated_Model/Results")

connector.upload(name="elements_to_gcs_prod", container="b76c14154ee4d5d351c531a0afacb1647570f3a14ab1de74", source_path="/data/GCP/Aggregated_Model/Logs/", target_path="dbcure/Aggregated_Model/Logs")

connector.upload(name="elements_to_gcs_prod", container="b76c14154ee4d5d351c531a0afacb1647570f3a14ab1de74", source_path="/data/GCP/Aggregated_Model/Insights/", target_path="dbcure/Aggregated_Model/Insights")

connector.upload(name="elements_to_gcs_prod", container="b76c14154ee4d5d351c531a0afacb1647570f3a14ab1de74", source_path="/data/GCP/Aggregated_Model/Halo/", target_path="dbcure/Aggregated_Model/Halo")

connector.upload(name="elements_to_gcs_prod", container="b76c14154ee4d5d351c531a0afacb1647570f3a14ab1de74", source_path="/data/GCP/Aggregated_Model/Cann/", target_path="dbcure/Aggregated_Model/Cann")

#Time_dp_end = Sys.time()
