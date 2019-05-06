# pH_correction_functions.R
#   Functions used in the code "app.R" in the program "pH_correction_v101"
#
#   List of functions:
#
# grep.start.mult
# calc.param.sel
# plot.profiles
# plot.one.profile
# plot.prof.graph
# settings.list.create
# get.jc.patterns
# calc.valid.data.df
#

# Function grep.start.mult *****************************************************
# Finds the first index of the string (in str_list) starting 
#   with a one of the elements of the "sub_str" (which may be several strings)
#  Returns NA if nothing was found
grep.start.mult <- function ( sub_str, str_list ) {
  #
  #indx_sel <- which( substr( str_list, 1, nchar(sub_str) ) %in% sub_str )
  indx_sel <- apply( as.data.frame( sub_str, stringsAsFactors = FALSE), 1, 
                     function(x) { grep( x, substr( str_list, 1, nchar(x) ) ) } )
  indx_sel <- as.numeric( indx_sel )
  indx_sel <- indx_sel[ is.finite( indx_sel ) ]
  if( length( indx_sel ) > 1 ) indx_sel <- indx_sel[1]
  if( length( indx_sel ) == 0 ) indx_sel <- NA
  #
  return( indx_sel )
}
# End of function grep.start.mult **********************************************

# Function calc.param.sel *****************************************************
# Calculates data frame param.sel, including columns:
#     col.name and k.col (the name(s) the column should start and its number)
calc.param.sel <- function( param.list, Param.names, data.col.names ) {
  # browser()
  n.param <- length( param.list )
  param.sel <- data.frame( k.col = rep( NA, n.param ), col.name = character( n.param ),
                           stringsAsFactors = FALSE )
  rownames( param.sel ) <- param.list
  for( k.param.sel in 1:n.param ) {
    k.param <- which( Param.names$Parameter == rownames( param.sel )[ k.param.sel ] )
    if( length( k.param ) == 0 ) {
      param.sel <- NULL    
    } else {
      param.sel$k.col[ k.param.sel ] <- grep.start.mult( Param.names$ParamNameStarts[ k.param ], data.col.names )
      if( is.na( param.sel$k.col[ k.param.sel ] ) ) {
        param.sel$col.name[ k.param.sel ] <- paste( Param.names$ParamNameStarts[ k.param ], collapse = "," ) 
      } else {
        param.sel$col.name[ k.param.sel ] <- paste( data.col.names[ param.sel$k.col[ k.param.sel ] ], collapse = "," )  
      }
    }
  }
  # browser()
  #
  return( param.sel )
}
# End of function calc.param.sel **********************************************


# Function plot.profiles *******************************************************
plot.profiles <- function( X, Z, StnID, lwd = 1, col = "#000000" ) {
  StnID.list <- data.frame( StnID = unique( StnID ) )
  #plot.one.profile( StnID.list[1], X, Z, StnID, lwd = 1, col = "#000000" )
  apply( X = StnID.list, MARGIN = 1, FUN = plot.one.profile, X, Z, StnID, 
         1, col )
}
# End of function plot.profiles ************************************************

# Function plot.one.profile ****************************************************
plot.one.profile <- function( StnID.plot, X, Z, StnID, lwd = 1, col = "#000000" ) {
  StnID.plot <- strsplit( StnID.plot, split = " " )[[1]][1]
  indx.stn <- ( StnID == StnID.plot )
  X.plot <- X[indx.stn]
  Z.plot <- Z[indx.stn]
  indx.sort <- order( Z.plot )
  X.plot <- X.plot[ indx.sort ]
  Z.plot <- Z.plot[ indx.sort ]
  lines( X[indx.stn], Z[indx.stn], lwd = lwd, col = col )
}
# End of function plot.one.profile *********************************************

# Function plot.prof.graph ****************************************************
plot.prof.graph <- function( x.data, z.data, StnID, prof.Z.ranges, main, ylab ) {
  prof.Z.lims <- data.frame( 
    x = extendrange( x.data, f = 0.1 ),
    y = extendrange( z.data, f = 0.1 ) )
  if( !is.null( prof.Z.ranges$x ) & !is.null( prof.Z.ranges$y ) ) {
    prof.Z.lims$x = prof.Z.ranges$x
    prof.Z.lims$y = prof.Z.ranges$y
  }
  plot( x.data, z.data, main = main,
        xlim = prof.Z.lims$x, 
        ylim = rev( prof.Z.lims$y ),
        xlab = "", ylab = ylab,
        font.main = 2, font.lab = 2, cex.main = 1.6, cex.lab = 1.2 )
  plot.profiles( x.data, z.data, StnID, 1, "#000000" )
}
# End of function plot.prof.graph *********************************************

settings.list.create <- function( CTD.param.names, BB.param.names, pH.corr.settings.R ) {
  indx.NA <- grepl( "indx_NA", pH.corr.settings.R$ShortName )
  indx.dTime <- which( grepl( "dTime", pH.corr.settings.R$ShortName ) )
  indx.dZ <- which( grepl( "dZ", pH.corr.settings.R$ShortName ) )
  settings = list(
    CTD.colnames = CTD.param.names[,1:3],
    BB.colnames = BB.param.names[,1:3],
    indx_NA = pH.corr.settings.R$Value[indx.NA],
    dTime = pH.corr.settings.R$Value[indx.dTime[1]],
    dZ = pH.corr.settings.R$Value[indx.dZ[1]]
  )
  # browser()
}

get.jc.patterns <- function( pattern, colnames ) {
  # For each pattern, get the index of the first fitting column
  n.patterns <- length( pattern )
  jc.param <- rep( NA, n.patterns )
  for( k.pattern in 1:n.patterns ) {
    jc.param[ k.pattern ] <- grep( pattern[k.pattern], colnames )[1]
  }
  jc.param[ length( jc.param ) == 0 ] <- NA
  #
  return( jc.param )
}
  
calc.valid.data.df <- function( CTD.data ) {

  Valid.data <- data.frame( Parameter = colnames( CTD.data ),
                            Samples = NA, Stations = NA, Profiles = NA,
                            Min = NA, Max = NA, stringsAsFactors = FALSE )
  Valid.data$Samples <- apply( !is.na( CTD.data ), 2, sum )
  calc.N.v.stns <- function( CTD.data.column, StnID ) {
    indx.val <- !is.na( CTD.data.column )
    N.v.stns <- length( unique( StnID[ indx.val ] ) )
    return( N.v.stns )
  }
  Valid.data$Stations <- apply( CTD.data, 2, calc.N.v.stns,
                                CTD.data$StnID )
  Valid.data$Profiles <- apply( CTD.data, 2, calc.N.v.stns,
                                CTD.data$Profile )
  Valid.data$Min <- suppressWarnings( apply( CTD.data, 2, min, na.rm = TRUE ) )
  Valid.data$Max <- suppressWarnings( apply( CTD.data, 2, max, na.rm = TRUE ) )
  #
  return( Valid.data ) 

}
  