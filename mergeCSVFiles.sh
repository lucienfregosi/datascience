#!/bin/bash
OutFileName="mergeCSV.csv"
i=0                                        # Initialisation d'un compteur
for filename in /home/ig2i/test/lucien/output/commande_nic_replaced_1477833283770/*.csv; # On va chercher tous les csv du dossier
do
  if [ $i -eq 0 ];
  then
    cat $filename >> $OutFileName # si c'est le premier on copie le header
  else
    cat $filename | sed 1d >>  $OutFileName # si ca n'est pas le premier on prend pas le première ligne
  fi
  ((i++))                                 # Increase the counter
done
