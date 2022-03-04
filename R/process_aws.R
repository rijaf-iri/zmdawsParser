#' Process CAMPBELL data.
#'
#' Get the data from CAMPBELL database, parse and insert into ADT database.
#' 
#' @param dirAWS full path to the directory containing the AWS_DATA folder on Campbell server.\cr
#' @param dirUP full path to the directory containing the AWS_DATA folder on ADT server.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.\cr
#' @param upload logical, if TRUE the data will be uploaded to ADT server.
#' 
#' @export

process.campbell <- function(dirAWS, dirUP = NULL, upload = TRUE){
    on.exit(RODBC::odbcClose(conn))

    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "CAMPBELL")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_campbell.txt")

    conn <- connect.campbell(dirAWS)
    if(is.null(conn)){
        msg <- "An error occurred when connecting to LoggerNet database"
        format.out.msg(msg, logPROC)
        return(1)
    }

    ret <- try(get.campbell.data(conn, dirAWS, dirUP, upload), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        msg <- paste(ret, "Getting CAMPBELL data failed")
        format.out.msg(msg, logPROC)
    }

}

