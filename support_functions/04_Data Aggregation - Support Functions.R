
# make a short hand for NOT IN function
'%!in%' <- Negate('%in%')

# I hate R matrix notation - impossible to read quickly

# Median - for integer responses we take the median of the answers
median_calc <- function(responses) {
  if(all(is.na(responses))) {
    return("")
  } else
    return(median(responses %>% na.omit()))
}

# if any KI checked an response for a multiple choice question, check it at the aggregated level
keep_any_yes  <- function(responses) {
  if(all(is.na(responses))) {
    return("")
  } else {
    empty_check <- responses[responses %!in% c("")]
    if(length(empty_check)  ==  0) {
      return("")
    } else {
      yes_check = max(responses %>% na.omit())
    }
    return(yes_check)
  }
}


# same as the above function - but instead of checking for 1s and 0s, we check for "yes" within lists of "yes" and "no"
keep_any_yes_text  <- function(responses) {
  if(all(is.na(responses))) {
    return("")
  } else {
    empty_check <- responses[responses %!in% c("")]
    if(length(empty_check)  ==  0) {
      return("")
    } else {
      yes_check <- as.integer("yes" %in% responses)
    }
    return(yes_check)
  }
}


# choose the response with the highest frequency, if there's a tie then label it "No Consensus"
aok_mode  <- function(x) {
  if(all(is.na(x))) {return("")}
  else{
    x <- x[x %!in% c("")]
    if(length(x)  ==  0) {return("")}
    else{
      x = table(x)
      modes = sum(x == max(x))
      if(modes  ==  1) {return(names(which(x  ==  max(x))))}
      else {return("NC")}
    }}}

aok_yes  <- function(x) {
  if(all(is.na(x))) {return("")}
  else{
    x  <-  x[x %!in% c("")]
    if(length(x)  ==  0) {return("")}
    else{
      x = table(x)
      modes = sum(x  ==  max(x))
      if(modes  ==  1) {return(names(which(x  ==  max(x))))}
      else if(modes > 1) {
        if("yes" %in% names(which(x  ==  max(x)))) {"yes"} else {return("NC")}
      }
      else {return("NC")}
    }}}

aok_no  <- function(x) {
  if(all(is.na(x))) {return("")}
  else{
    x  <-  x[x %!in% c("")]
    if(length(x)  ==  0) {return("")}
    else{
      x = table(x)
      modes = sum(x  ==  max(x))
      if(modes  ==  1) {return(names(which(x  ==  max(x))))}
      else if(modes > 1) {
        if("no" %in% names(which(x  ==  max(x)))) {"no"} else {return("NC")}
      }
      else {return("NC")}
    }}}

aok_true  <- function(x) {
  if(all(is.na(x))) {return("")}
  else{
    x  <-  x[x %!in% c("")]
    if(length(x)  ==  0) {return("")}
    else{
      x = table(x)
      modes = sum(x  ==  max(x))
      if(modes  ==  1) {return(names(which(x  ==  max(x))))}
      else if(modes > 1) {
        if(TRUE %in% names(which(x  ==  max(x)))) {TRUE} else {return("NC")}
      }
      else {return("NC")}
    }}}

 
# for testing and individual question aggregation method
# db <- data_split[[300]]
# question_name <- agg_method$variable
# q_smult <- q_smult
# agg_method <- agg_method
# x <- "people_moved_out"
# x <- "reason_moved"

# barriers_water	barriers_water_other


apply_aok <- function(question_name, db, q_smult, agg_method) {
  lapply(question_name, function(x, db, q_smult, agg_method) {
    
    method <- agg_method$aggregation[agg_method$variable == x] %>% unique
    
    if(length(method) == 0) {
      # if the piece of data isn't aggregated, then we concatenate all of the answers with "-/-" but why??
      myfun <- function(x) paste(x, collapse="-/-")
    } else {
      if(method == "aok_mode") { 
        myfun <- aok_mode
      } else if(method  ==  'median') {
        myfun <- median_calc
      } else if(method == "aok_yes") {
        myfun <- aok_yes
      } else if(method == "keep_any_yes") {
        myfun <- keep_any_yes
      } else if(method == "keep_any_yes_text") {
        myfun <- keep_any_yes_text
      } else if(method == "aok_no") {
        myfun <- aok_no
      } else if(method == "aok_true") {
        myfun <- aok_true     
      } else if(method == "first") {
        myfun <- function(x) first(x)
      } else if(method == "keep_all") {
        myfun <- function(x) paste(x, collapse="-/-")
      } else if(method == "hh_level") {
        myfun <- function(x) {return("hh_level")}
      }
    }
    
    if(x %in% q_smult) {
      out <- db[, grep(paste0(x, "/"), names(db))] %>% lapply(myfun) %>% as.data.frame(check.names = F)
    } else {
      out <- db[[as.character(x)]] %>% myfun %>% as.data.frame()
      names(out) <- x
    } 
    return(out)
  }, db=db, q_smult=q_smult, agg_method=agg_method)
} %>% bind_cols