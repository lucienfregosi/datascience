//////////////////////////////////////////////////
// Nom du fichier       :  delete_nic_and_primarykey_null.scala
// Description concise  :  delete row where nic or primary key is null
// Date création : 				 19/10/2016
// Nom du dev   :					 Lucien
////////////////////////////////////////////////


import org.apache.spark.*
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

val sqlContext = spark
.builder()
.appName("NicHandle sort CSV")
.master("local")
.getOrCreate()

// ON obtient un data frame
val reader = sqlContext.read
.format("com.databricks.spark.csv")
.option("delimiter", ";")
.option("quote", "|")
.option("header", "true")

val reader2 = sqlContext.read
.format("com.databricks.spark.csv")
.option("delimiter", ",")
.option("quote", "|")
.option("header", "true")

// Le fonctionnement est le suivant :
// - Pour la table nichande on vérifie si le nic est non null
// - Pour les tables principales (paiement, factures, commandes, remboursement) on vérifie si le nic est non null et si la clef primaire est non null
// - Pour les tables de détails, on vérifie si la clef étrangère (vers la table principale) est non null (et si elle est présente dans la table principale) et si la clef primaire est non null


// On load les différents CSV
val dfNic = reader2.load("/home/ig2i/data/n2.1/nichandle.csv")

val dfCommande = reader.load("/home/ig2i/data/v2/commande.csv")
val dfPaiement = reader2.load("/home/ig2i/data/n2.1/paiement.csv")
val dfFacture  = reader2.load("/home/ig2i/data/n2.1/facture.csv")
val dfRefund   = reader.load("/home/ig2i/data/v2/refund.csv")

val dfCommandeDetail = reader.load("/home/ig2i/data/v2/commandedetail.csv")
val dfFactureDetail  = reader.load("/home/ig2i/data/v2/facturedetail.csv")
val dfRefundDetail   = reader.load("/home/ig2i/data/v2/refunddetail.csv")

//Pour la table nichande on vérifie si le nic est non null et si //
print("Lignes avant nettoyage nichandle:")
//dfNic.count()
val dfNicClean   = dfNic.filter(not($"NIC" === 0)).filter(not($"NIC" like "null"))
print("Lignes après nettoyage nichandle:")
//dfNicClean.count()

//Pour les tables principales (paiement, factures, commandes, remboursement) on vérifie si le nic est non null et si la clef primaire est non null
//Pour la table nichande on vérifie si le nic est non null et si //
print("Lignes avant nettoyage commandes:")
//dfCommande.count()
val dfCommandeClean   = dfCommande.filter(not($"1 NICPROPRIO" === 0))
.filter(not($"1 NICPROPRIO" like "null"))
.filter(not($"2 CLECOMMANDE" === 0))
.filter(not($"2 CLECOMMANDE" like "null"))
print("Lignes après nettoyage commandes:")
//dfCommandeClean.count()

print("Lignes avant nettoyage factures:")
//dfFacture.count()
val dfFactureClean   = dfFacture.filter(not($"CLECOMMANDE" === 0))
.filter(not($"CLECOMMANDE" like "null"))
.filter(not($"NIC" === 0))
.filter(not($"NIC" like "null"))
.filter(not($"CLEFACTURE" === 0))
.filter(not($"CLEFACTURE" like "null"))
print("Lignes après nettoyage factures:")
//dfFactureClean.count()

print("Lignes avant nettoyage paiements:")
//dfPaiement.count()
val dfPaiementClean   = dfPaiement.filter(not($"CLEPAIEMENT" === "0"))
.filter(not($"CLEPAIEMENT" like "null"))
.filter(not($"NIC" === 0))
.filter(not($"NIC" like "null"))
print("Lignes après nettoyage paiements:")
//dfPaiementClean.count()

print("Lignes avant nettoyage remboursements:")
//dfRefund.count()
val dfRefundClean   = dfRefund.filter(not($"0 ID" === 0))
.filter(not($"0 ID" like "null"))
.filter(not($"3 NIC" === 0))
.filter(not($"3 NIC" like "null"))
print("Lignes après nettoyage remboursements:")
//dfRefundClean.count()

print("Lignes avant nettoyage commande détails:")
//dfCommandeDetail.count()
val dfCommandeDetailClean   = dfCommandeDetail.filter(not($"0 CLE" === "0"))
.filter(not($"0 CLE" like "null"))
.filter(not($"1 CLECOMMANDE" === 0))
.filter(not($"1 CLECOMMANDE" like "null"))
print("Lignes après nettoyage commande détails:")
//dfCommandeDetailClean.count()

print("Lignes avant nettoyage facture détails:")
//dfFactureDetail.count()
val dfFactureDetailClean   = dfFactureDetail.filter(not($"9 CLEFACTURE" === 0))
.filter(not($"9 CLEFACTURE" like "null"))
.filter(not($"13 CLE" === 0))
.filter(not($"13 CLE" like "null"))
print("Lignes après nettoyage commande détails:")
//dfFactureDetailClean.count()

print("Lignes avant nettoyage remboursements détails:")
//dfRefundDetail.count()
val dfRefundDetailClean   = dfRefundDetail.filter(not($"0 ID" === 0))
.filter(not($"0 ID" like "null"))
.filter(not($"1 REFUNDID" === 0))
.filter(not($"1 REFUNDID" like "null"))
print("Lignes après nettoyage remboursements détails:")
//dfRefundDetailClean.count()


On sauvegarde tous les fichiers dans le répertoire de sortie

dfNicClean.repartition(1).coalesce(1)
.write
.format("com.databricks.spark.csv")
.option("header", "true")
.save("output/nic_" + System.currentTimeMillis())

dfCommandeClean.repartition(1).coalesce(1)
.write
.format("com.databricks.spark.csv")
.option("header", "true")
.save("output/commande_" + System.currentTimeMillis())

dfFactureClean.repartition(1).coalesce(1)
.write
.format("com.databricks.spark.csv")
.option("header", "true")
.save("output/facture_" + System.currentTimeMillis())

dfRefundClean.repartition(1).coalesce(1)
.write
.format("com.databricks.spark.csv")
.option("header", "true")
.save("output/refund_" + System.currentTimeMillis())


dfCommandeDetail.repartition(1).coalesce(1)
.write
.format("com.databricks.spark.csv")
.option("header", "true")
.save("output/commandeDetail_" + System.currentTimeMillis())

dfFactureDetail.repartition(1).coalesce(1)
.write
.format("com.databricks.spark.csv")
.option("header", "true")
.save("output/factureDetail_" + System.currentTimeMillis())

dfRefundDetail.repartition(1).coalesce(1)
.write
.format("com.databricks.spark.csv")
.option("header", "true")
.save("output/refundDetail_" + System.currentTimeMillis())

dfPaiementClean.repartition(1).coalesce(1)
.write
.format("com.databricks.spark.csv")
.option("header", "true")
.save("output/paiement_" + System.currentTimeMillis())




//exit scala interpretor
System.exit(0)
