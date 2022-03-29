
get.campbell.data <- function(conn, dirAWS, dirUP = NULL, upload = TRUE){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "campbell_aws_list.csv")
    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "campbell_parameters_table.csv")
    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "CAMPBELL")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "CAMPBELL")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    awsLOG <- file.path(dirLOG, "AWS_LOG.txt")

    if(upload){
        ssh <- readRDS(file.path(dirAWS, "AWS_DATA", "AUTH", "adt.cred"))
        session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)
        if(inherits(session, "try-error")){
            logUpload <- file.path(dirAWS, "AWS_DATA", "LOG", "CAMPBELL", "processing_campbell.txt")
            msg <- paste(session, "Unable to connect to ADT server\n")
            format.out.msg(msg, logUpload)

            upload <- FALSE
        }

        dirADT <- file.path(dirUP, "AWS_DATA", "DATA", "CAMPBELL")
        ssh::ssh_exec_wait(session, command = c(
            paste0('if [ ! -d ', dirADT, ' ] ; then mkdir -p ', dirADT, ' ; fi')
        ))
    }

    awsInfo <- utils::read.table(awsFile, header = TRUE, sep = ",", na.strings = "",
                                 stringsAsFactors = FALSE)
    lastDate <- as.POSIXct(as.integer(awsInfo$last), origin = origin, tz = tz)
    lastDate <- format(lastDate, "%Y-%m-%d %H:%M:%S.0000000")
    awsID <- paste0(awsInfo$id, "-", awsInfo$name)
    awsTable <- paste0(awsInfo$sqlTableName, "_", awsInfo$tableExtension)
    varTable <- var.network.table(varFile)

    for(j in seq_along(awsTable)){
        query <- paste0("SELECT * FROM [", awsTable[j], "] WHERE TmStamp > '", lastDate[j], "'")
        qres <- try(RODBC::sqlQuery(conn, query), silent = TRUE)
        if(inherits(qres, "try-error")){
            msg <- paste("Unable to get data for", awsID[j])
            format.out.msg(msg, awsLOG)
            next
        }
        if(nrow(qres) == 0) next
        out <- parse.campbell.data(qres, awsID[j], varTable)
        if(is.null(out)) next

        awsInfo$last[j] <- max(out$obs_time)

        locFile <- paste0(awsID[j], "_", paste(range(out$obs_time), collapse = "_"))
        locFile <- file.path(dirOUT, locFile)
        saveRDS(out, locFile)

        if(upload){
            adtFile <- file.path(dirADT, basename(locFile))
            ssh::scp_upload(session, locFile, to = adtFile, verbose = FALSE)
        }
    }

    utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
                       row.names = FALSE, quote = FALSE)

    return(0)
}
