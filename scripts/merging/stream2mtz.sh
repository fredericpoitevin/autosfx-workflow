#!/bin/bash

# STREAM=
# UCELL=
: ${JOBNAME:="testing"}
: ${PGROUP:="222"}
: ${SGNUM:="19"}
: ${MAXADU:="60000"}
: ${WAVELA:="0.977"}
: ${MERGE:="TRUE"}
: ${FRIEDEL:="TRUE"}
: ${WORKDIR:="./"}
: ${HIGHRES:="1.3"}


## get parameters from the inputs
for i in "$@"; do
    if [[ ${i:0:7} = "stream:" ]]; then
        STREAM=${i:7:1000} 
    elif [[ ${i:0:8} = "jobname:" ]]; then
        JOBNAME=${i:8:1000} 
    elif [[ ${i:0:6} = "ucell:" ]]; then
        UCELL=${i:6:1000}
    elif [[ ${i:0:7} = "pgroup:" ]]; then
        PGROUP=${i:7:1000}
    elif [[ ${i:0:6} = "sgnum:" ]]; then
        SGNUM=${i:6:1000}
    elif [[ ${i:0:7} = "maxadu:" ]]; then
        MAXADU=${i:7:1000}
    elif [[ ${i:0:7} = "wavela:" ]]; then
        WAVELA=${i:7:1000}
    elif [[ ${i:0:6} = "merge:" ]]; then
        MERGE=${i:6:1000}
    elif [[ ${i:0:8} = "friedel:" ]]; then
        FRIEDEL=${i:8:1000}
    elif [[ ${i:0:8} = "workdir:" ]]; then
        WORKDIR=${i:8:1000}
    elif [[ ${i:0:8} = "highres:" ]]; then
        HIGHRES=${i:8:1000}
    else
        :
    fi
done


if [ ! -f "$UCELL" ]; then
    echo "!!! unit cell doesn't exist: $UCELL"
    exit
fi
if [ ! -f "$STREAM" ]; then
    echo "!!! stream doesn't exist: $STREAM"
    exit
fi

FCELL=`readlink -f $UCELL`
echo "... unit cell = $FCELL"
WORKDIR=`readlink -f $WORKDIR`
echo "... work dir = $WORKDIR"
STREAM=`readlink -f $STREAM`
echo "... stream $STREAM"


echo "### Running process hkl"
echo "... STREAM=$STREAM"
echo "... WORKDIR=$WORKDIR"
echo "... JOBNAME=$JOBNAME"
echo "... PGROUP=$PGROUP"
echo "... MAXADU=$MAXADU"

echo ">>> cd $WORKDIR && process_hkl -i $STREAM -o $JOBNAME.hkl -y $PGROUP --max-adu=$MAXADU --scale"


cd $WORKDIR && process_hkl -i $STREAM -o $JOBNAME.hkl -y $PGROUP --max-adu=$MAXADU --scale



echo "### Running create-xscale"
if [ -f "$WORKDIR/"$JOBNAME"_create_xscale" ]; then
    cd $WORKDIR && rm $JOBNAME"_create_xscale"
fi


head -n 7 /reg/data/ana03/scratch/zhensu/sfxtools/create-xscale >> $WORKDIR'/'$JOBNAME'_create_xscale'



LA="$(cat $FCELL | grep "a ="* | grep " A")" 
LB="$(cat $FCELL | grep "b ="* | grep " A")" 
LC="$(cat $FCELL | grep "c ="* | grep " A")"
LAL="$(cat $FCELL | grep "al ="* | grep " deg")" 
LBE="$(cat $FCELL | grep "be ="* | grep " deg")" 
LGA="$(cat $FCELL | grep "ga ="* | grep " deg")" 

LATTA=${LA:4:5}
LATTB=${LB:4:5}
LATTC=${LC:4:5}
LATTALPHA=${LAL:5:3}
LATTBETA=${LBE:5:3}
LATTGAMMA=${LGA:5:3}

echo '### making create-xscale:' $WORKDIR'/'$JOBNAME'_create_xscale'
echo 'printf("!FORMAT=XDS_ASCII   MERGE='$MERGE"   FRIEDEL'S_LAW="$FRIEDEL'\n");' >> $WORKDIR'/'$JOBNAME'_create_xscale'
echo 'printf("!SPACE_GROUP_NUMBER='$SGNUM'\n");' >> $WORKDIR'/'$JOBNAME'_create_xscale'
echo 'printf("!UNIT_CELL_CONSTANTS=      '$LATTA'    '$LATTB'   '$LATTC' '$LATTALPHA' '$LATTBETA'    '$LATTGAMMA'\n");' >> $WORKDIR'/'$JOBNAME'_create_xscale'
echo 'printf("!NUMBER_OF_ITEMS_IN_EACH_DATA_RECORD=5\n");' >> $WORKDIR'/'$JOBNAME'_create_xscale'
echo 'printf("!X-RAY_WAVELENGTH= '$WAVELA'\n");' >> $WORKDIR'/'$JOBNAME'_create_xscale'
tail -n 33 /reg/data/ana03/scratch/zhensu/sfxtools/create-xscale >> $WORKDIR'/'$JOBNAME'_create_xscale'

cd $WORKDIR && chmod +x $JOBNAME'_create_xscale'

echo "... lattace a = $LATTA"
echo "... lattace b = $LATTB"
echo "... lattace c = $LATTC"
echo "... lattace al = $LATTALPHA"
echo "... lattace be = $LATTBETA"
echo "... lattace ga = $LATTGAMMA"
echo "... MERGE = $MERGE"
echo "... FRIEDEL = $FRIEDEL"
echo "... Spage Group NUM = $SGNUM"
echo "... wavelength_A = $WAVELA"
echo "... create-xscale = $WORKDIR/"$JOBNAME"_create_xscale"

echo ">>> cd $WORKDIR && ./"$JOBNAME"_create_xscale "$JOBNAME".hkl >> "$JOBNAME"_xscale.hkl"


RUNCMD="./"$JOBNAME"_create_xscale "$JOBNAME".hkl >> "$JOBNAME"_xscale.hkl"
cd $WORKDIR && eval "$RUNCMD"





echo "### Running xdsconv"

if [ -f "$WORKDIR/XDSCONV.INP" ]; then
    cd $WORKDIR && rm XDSCONV.INP 
fi


echo "... MERGE = $MERGE"
echo "... FRIEDEL = $FRIEDEL"
echo "... HIGHRES = $HIGHRES"


echo 'INPUT_FILE='$JOBNAME'_xscale.hkl XDS_ASCII' >> $WORKDIR/XDSCONV.INP
echo 'INCLUDE_RESOLUTION_RANGE=50 '$HIGHRES >> $WORKDIR/XDSCONV.INP
echo '                  ' >> $WORKDIR/XDSCONV.INP
echo 'OUTPUT_FILE='$JOBNAME'_xscale_shelx.cv  CCP4_I' >> $WORKDIR/XDSCONV.INP
echo '                  ' >> $WORKDIR/XDSCONV.INP
echo "FRIEDEL'S_LAW=$FRIEDEL" >> $WORKDIR/XDSCONV.INP
echo 'MERGE='$MERGE >> $WORKDIR/XDSCONV.INP


echo ">>> cd $WORKDIR && xdsconv"

cd $WORKDIR && xdsconv &&
 f2mtz HKLOUT temp.mtz<F2MTZ.INP
 cad HKLIN1 temp.mtz HKLOUT output_file_name.mtz<<EOF
 LABIN FILE 1 ALL
 DWAVELENGTH FILE 1 1    $WAVELA
 END
EOF


