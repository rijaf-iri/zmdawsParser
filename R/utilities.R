
Sys.setenv(TZ = "Africa/Lusaka")

getObsId <- function(qres){
    paste(qres$network, qres$id, qres$height,
          qres$obs_time, qres$var_code,
          qres$stat_code, sep = "_")
}

var.network.table <- function(varFile){
    varTable <- utils::read.table(varFile, sep = ",", na.strings = "",
                                  header = TRUE, stringsAsFactors = FALSE)
    varTable <- varTable[!is.na(varTable$parameter_code), , drop = FALSE]
    varTable <- lapply(seq_along(varTable$parameter_code), function(i){
        vr <- strsplit(varTable$parameter_code[i], "\\|")[[1]]
        x <- varTable[i, , drop = FALSE]
        if(length(vr) > 1){
            x <- x[rep(1, length(vr)), ]
            x$parameter_code <- vr
        }
        x
    })
    varTable <- do.call(rbind, varTable)

    return(varTable)
}

format.out.msg <- function(msg, logfile, append = TRUE){
    ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n",
             "*********************************\n")
    cat(ret, file = logfile, append = append)
}

connect.DBI <- function(con_args, drv){
    args <- c(list(drv = drv), con_args)
    con <- try(do.call(DBI::dbConnect, args), silent = TRUE)
    if(inherits(con, "try-error")) return(NULL)
    con
}

connect.RODBC <- function(con_args){
    args <- paste0(names(con_args), '=', unlist(con_args))
    args <- paste(args, collapse = ";")
    args <- list(connection = args, readOnlyOptimize = TRUE)
    con <- try(do.call(RODBC::odbcDriverConnect, args), silent = TRUE)
    if(inherits(con, "try-error")) return(NULL)
    con
}

connect.campbell <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "campbell.con")
    campbell <- readRDS(ff)
    conn <- connect.RODBC(campbell$connection)
    if(is.null(conn)){
        Sys.sleep(3)
        conn <- connect.RODBC(campbell$connection)
        if(is.null(conn)) return(NULL)
    }

    return(conn)
}
