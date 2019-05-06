DENSITY <- function( S, T_C) {
  # Equation of State is from Millero & Poisson (1981) DSR V28: 625-629.
# 
#   INPUT:       	Salinity (S) in g/kg or pss.
#   Temperature (T) in degrees C.
#   
#   OUTPUT:	Density [rho] in g/cc.
#   
#   DEFINE CONSTANTS FOR EQUATION OF STATE
#   
  R0 <-  9.99842594E2
  R1 <- 6.793952E-2
  R2 <- -9.095290E-3
  R3 <-  1.001685E-4
  R4 <- -1.120083E-6
  R5 <-  6.536332E-9
  
  A0 <-  8.24493E-1
  A1 <- -4.0899E-3
  A2 <-  7.6438E-5
  A3 <- -8.2467E-7
  A4 <-  5.3875E-9
  
  B0 <- -5.72466E-3
  B1 <-  1.0227E-4
  B2 <- -1.6546E-6
  
  C  <-  4.8314E-4
  
  # CALCULATE RHO
  
  SR <- sqrt(S)
  
  RHO0 <- R0 + T_C * (R1 + T_C * (R2 + T_C * (R3 + T_C *(R4 + T_C * R5))))
  
  A <- A0 + T_C * (A1 + T_C * (A2 + T_C * (A3 + T_C * A4)))
  B <- B0 + T_C * (B1 + T_C * B2)
  RHO <- RHO0 + S * (A + B * SR + C * S)
  
  # CONVERT KG/M3 TO g/cc
  
  DENSITY <- RHO / 1000
  
  return( DENSITY )
  
}

pH_nbs2total <- function( S, T_C, pH_NBS ) {
  # CONVERT pH (ON NBS SCALE) TO pH on TOTAL SCALE
  # source: https://www.mbari.org/products/research-software/visual-basic-for-excel-oceanographic-calculations/
  #   link: https://www.mbari.org/wp-content/uploads/2016/03/pH_nbs2total.txt
  # 
  # This code was adapted from the work of others:
  #     CO2SYS.exe was originally written by Ernie Lewis and Doug Wallace.
  #     It was converted to Matlab by Richard E. Zeebe & Dieter A. Wolf-Gladrow.
  #     Parts were extracted and modified by Rachel M. Dunk.
  # 
  # I am indebted to all of them for their initial development of this code.
  # 
  # FIRST: convert pH_NBS to pH_SWS
  #   aH = 10^(-pH_NBS) = fH*H_SWS
  #   fH = activity coefficient of H+ ion & includes liquid junction effects 
  #   The fit used in CO2SYS is valid for salinity 20-40 (from Takahashi et al,
  #     Chapter 3 in GEOSECS Pacific Expedition,v.3, 1982, p. 80).
  #   For future ref, CO2SYS assumes fH is independent of P
  #                                                        
  #  Convert T to degree K
  T_K <- T_C + 273.15                                               
  
  # Convert pH to H+ concentration
  aH <- 10 ^ -pH_NBS
  fH <- 1.2948 - .002036 * T_K
  fH <- fH + (.0004607 - 1.475E-06 * T_K) * S^2
  H  <- aH / fH

#  On NBS scale, aH is in mol/dm3_H2O, whereas H_SWS is in mol/kg_SW
#    to convert units we need the density of SW at given S,T
#  Call DENSITY.bas (author=ETP), calculates rho(S,T,P=1atm) in g/cm3
#    NOTE: in this function T is in deg C
#    Equation of State is from Millero & Poisson (1981) DSR V28: 625-629.
  
  rho_SW <- DENSITY( S, T_C)
  
  pH_SWS <- -log( H / rho_SW ) / log(10)
  
  # SECOND: convert pH_SWS to pH_total
  # CONVERT pH (on SEAWATER SCALE) TO pH on TOTAL SCALE
  
  H_SWS <- 10^( -pH_SWS )
  Hi <- 10^(- (pH_SWS + 0.01))
  F_T <- 0.00007 * S / 35
  lnK_F <- 874 / T_K - 9.68 + 0.111*S^0.5
  K_F <- exp( lnK_F )
  HF <- F_T / ( 1 + K_F/Hi )
  H_T <- H_SWS - HF
  
  pH_nbs2total <- - log( H_T ) / log(10)
  
  return( pH_nbs2total )
  
}




