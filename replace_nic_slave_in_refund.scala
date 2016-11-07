//////////////////////////////////////////////////
// Nom du fichier       :  replace_nic_slave_in_refund.scala
// Description concise  :
// Date création        :	 21 October 2016
// Nom du dev           :	 Lucien
////////////////////////////////////////////////

import org.apache.spark._
import org.apache.spark.{SparkContext, SparkConf}

//config
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//extract csv file
val CSVNicHeader = sc.textFile("/home/ig2i/data/n2.2/nichandle.csv")
//store header
val header = CSVNicHeader.first()
//remove header
val CSVNic = CSVNicHeader.filter(line => line != header)

//create class
case class NicHandle(
	CLEEMAIL: String,
	EMAIL: String,
	NOM: String,
	PRENOM: String,
	ZIP: String,
	VILLE: String,
	DPT: String,
	PAYS: String,
	LANGUE: String,
	NIC: String,
	TELEPHONE: String,
	FAX: String,
	CLIENT : String,
	MARKETING : String,
	ORGANISATION : String,
	DATE_CREATION : String,
	DATE_MAJ : String,
	DATE_ENVOI_CODE : String,
	ACTIF : String,
	BILLINGCOUNTRY : String,
	RESELLER : String,
	FORKEDFROMNIC : String
)

// Lecture des différents dataframe pour les jointures
val dataFrameReader = sqlContext.read
.format("com.databricks.spark.csv")
.option("delimiter", ";")
.option("quote", "|")
.option("header", "true")
val dfRefund = dataFrameReader.load("/home/ig2i/data/v2/refund.csv")



// Création d'une instance de la calsse nic en splittant le csv avec un point virgule
// NB : _.split() <=> 'x => x.split()'
val RDDNic = CSVNic.map(_.split(",")).map(p => NicHandle(p(0).toString, p(1).toString,p(2).toString,p(3).toString,p(4).toString,p(5).toString,p(6).toString,p(7).toString,p(8).toString,p(9).toString,p(10).toString,p(11).toString,p(12).toString,p(13).toString,p(14).toString,p(15).toString,p(16).toString,p(17).toString,p(18).toString,p(19).toString,p(20).toString,p(21).toString))



// On crée un tableau de correspondance entre email et NIC
val RDDNicSlaveMaster = RDDNic.map( x => x.EMAIL -> x.NIC)
// Groupage sur les emails pour générer une séquence de NIC
.groupByKey()
// On garde seulement la séquence de nichandle
.map(x => x._2)
// On veut utiliser le premier nic comme clef de la séquence
.map(x => x.head -> x)
// On veut construire un tableau de correspondance entre les nics master et les nics slaves. 2 solutions :
// - Utiliser la fonction flat map avec un double map qui permet de crééer une équivalence dans la séquence pour ensuite le flat
// - Utiliser la fonction flat map values qui fait toute seule mais a voir
.flatMapValues( x => x)
// on swap pour avoir les nic a remplacer
.map( x => x.swap)
// On crée une dataframe de notre RDD pour pouvoir faires les jointures avec les dataframe commande, paiement, facture et refund
val dfNicSlaveMasterRefund = RDDNicSlaveMaster.toDF()

// Jointures avec les remboursements
val dfNicRefund = dfNicSlaveMasterRefund.join(dfRefund, dfNicSlaveMasterRefund("_1") === dfRefund("3 NIC"))
val dfRefundNicReplaced = dfNicRefund.select($"0 ID",$"1 ORDERID",$"2 BILLID",$"_2".alias("3 NIC"),$"4 TOTALPRICE",$"5 VAT",$"6 TOTALPRICEWITHVAT",$"7 DATE",$"8 VATRATE",$"9 TYPE",$"10 PERIODESTART",$"11 PERIODEEND",$"12 PASSWORD",$"13 TOTALPOINTBONUS",$"15 SIGNATUREVERSION",$"16 SIGNATUREVALUE",$"17 SYSTEMFACTU",$"18 ZONE",$"19 PAYSFACTU")
dfRefundNicReplaced.write
.format("com.databricks.spark.csv")
.option("header", "true")
// Sauvegarde des refunds dans le fichier de sortie
.save("output/refund_nic_replaced_" + System.currentTimeMillis())
//dfRefundNicReplaced.rdd.saveAsTextFile("test2");

//exit and not launch scala interpretor
System.exit(0)
