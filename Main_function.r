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
  