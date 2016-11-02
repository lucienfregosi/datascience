//////////////////////////////////////////////////
// Nom du fichier       :  merge_email.scala
// Description concise  :  Merge email to keep a nic master and to replace nic slave in facture commande, paiement, refund et facture
// Date création        :	 21 October 2016
// Nom du dev           :	 Lucien
////////////////////////////////////////////////

import org.apache.spark._
import org.apache.spark.{SparkContext, SparkConf}

//config
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//extract csv file
val CSVNicHeader = sc.textFile("/home/ig2i/data/v2/nichandle.csv")
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
val dfCommande = dataFrameReader.load("/home/ig2i/data/v2/commande.csv")
val dfFacture = dataFrameReader.load("/home/ig2i/data/v2/facture.csv")
val dfPaiement = dataFrameReader.load("/home/ig2i/data/v2/paiement.csv")
val dfRefund = dataFrameReader.load("/home/ig2i/data/v2/refund.csv")
val dfNic = dataFrameReader.load("/home/ig2i/data/v2/nichandle.csv")


// Création d'une instance de la calsse nic en splittant le csv avec un point virgule
// NB : _.split() <=> 'x => x.split()'
val RDDNic = CSVNic.map(_.split(";")).map(p => NicHandle(p(1).toString,p(2).toString,p(3).toString,p(4).toString,p(5).toString,p(6).toString,p(7).toString,p(8).toString,p(9).toString,p(10).toString,p(11).toString,p(12).toString,p(13).toString,p(14).toString,p(15).toString,p(16).toString,p(17).toString,p(18).toString,p(19).toString,p(20).toString,p(21).toString,p(22).toString))


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
val dfNicSlaveMasterCommande = RDDNicSlaveMaster.toDF()
val dfNicSlaveMasterPaiement = RDDNicSlaveMaster.toDF()
val dfNicSlaveMasterFacture  = RDDNicSlaveMaster.toDF()
val dfNicSlaveMasterRefund   = RDDNicSlaveMaster.toDF()

// Jointure avec les commandes
val dfNicCommande = dfNicSlaveMasterCommande.join(dfCommande, dfNicSlaveMasterCommande("_1") === dfCommande("6 NIC"))
val dfCommandeNicReplaced = dfNicCommande.select($"0 INTERNALTS",$"1 NICPROPRIO",$"2 CLECOMMANDE",$"3 MAINACLID",$"4 EXPIRATION",$"5 TVA",$"_2".alias("6 NIC"),$"7 TAUXTVA",$"8 NICBILL",$"9 NICTECH",$"10 TYPE",$"11 DATE",$"12 TOTALHT",$"13 VATDEFAULTREASON",$"14 NICADMIN",$"15 REMOTEIP",$"16 SECONDARYACLID",$"17 PAYS",$"18 NICPAYMENT",$"19 TOTALTTC",$"20 ZONE")
dfCommandeNicReplaced.write
.format("com.databricks.spark.csv")
.option("header", "true")
// Sauvegarde des commandes dans le fichier de sortie
.save("output/commande_nic_replaced_" + System.currentTimeMillis())

// Jointure avec les factures
val dfNicFacture = dfNicSlaveMasterFacture.join(dfFacture, dfNicSlaveMasterFacture("_1") === dfFacture("3 NIC"))
val dfFactureNicReplaced = dfNicFacture.select($"0 INTERNALTS",$"1 CLECOMMANDE",$"_2".alias("3 NIC"),$"4 TAUXTVA",$"5 TYPE",$"6 DATE",$"7 VATDEFAULTREASON",$"8 TOTALHT",$"9 CLEFACTURE",$"10 PERIODEDEBUT",$"11 PERIODEFIN",$"12 TOTALPOINTBONUS",$"14 SYSTEMFACTU",$"15 ZONE",$"16 PAYSFACTU")
dfFactureNicReplaced.write
.format("com.databricks.spark.csv")
.option("header", "true")
// Sauvegarde des factures dans le fichier de sortie
.save("output/facture_nic_replaced_" + System.currentTimeMillis())

// Jointures avec les remboursements
val dfNicRefund = dfNicSlaveMasterRefund.join(dfRefund, dfNicSlaveMasterRefund("_1") === dfRefund("3 NIC"))
val dfRefundNicReplaced = dfNicRefund.select($"0 ID",$"1 ORDERID",$"2 BILLID",$"_2".alias("3 NIC"),$"4 TOTALPRICE",$"5 VAT",$"6 TOTALPRICEWITHVAT",$"7 DATE",$"8 VATRATE",$"9 TYPE",$"10 PERIODESTART",$"11 PERIODEEND",$"12 PASSWORD",$"13 TOTALPOINTBONUS",$"15 SIGNATUREVERSION",$"16 SIGNATUREVALUE",$"17 SYSTEMFACTU",$"18 ZONE",$"19 PAYSFACTU")
dfRefundNicReplaced.write
.format("com.databricks.spark.csv")
.option("header", "true")
// Sauvegarde des refunds dans le fichier de sortie
.save("output/refund_nic_replaced_" + System.currentTimeMillis())

// Jointure avec les paiements
val dfNicPaiement = dfNicSlaveMasterPaiement.join(dfPaiement, dfNicSlaveMasterPaiement("_1") === dfPaiement("8 NIC"))
val dfPaiementNicReplaced = dfNicPaiement.select($"0 INTERNALTS",$"1 CLEPAIEMENT",$"2 BC",$"3 ETAT",$"4 DATE_PAIEMENT",$"5 DATE_DONE",$"6 DOMAINE",$"7 PLAN",$"_2".alias("8 NIC"),$"9 EMAIL",$"11 DATE_REMISE",$"12 COUNTRYRECEIVINGCASH",$"13 AMOUNTINCENTS",$"14 REMOTEIP",$"15 CREDITCARDTYPE",$"16 CASHSTATUS",$"21 DUEDATE",$"22 ZONE")
dfPaiementNicReplaced.write
.format("com.databricks.spark.csv")
.option("header", "true")
// Sauvegarde des paiements dans le fichier de sortie
.save("output/paiement_nic_replaced_" + System.currentTimeMillis())


// On va maintenant traiter le fichier NIC on veut faire un group by emails et ne garder que les premiers
// Pour ce faire même technique, tableau a deux entrées
 val RDDNicGroupByFirstNic = RDDNic.map( x => x.EMAIL -> x.NIC)
// On group par la key (les emails)
.groupByKey()
// On ne garde que les NICS
.map(x => x._2)
// On extrait le premier sur lequel on va faire la jointure
.map(x => x.head)

// On transforme le RDD en dataframe
val dfNicGroupByFirstNic = RDDNicGroupByFirstNic.toDF()

// On fait une jointure avec le dataframe des nic, on associe le premier nic d'un groupe d'email donné
val dfNicFirst = dfNicGroupByFirstNic.join(dfNic, dfNicGroupByFirstNic("_1") === dfNic("10 NIC"))
// On sélectionne toutes les colonnes qui ont été récupéré dans la jointure
val dfNicFirstFinal = dfNicFirst.select($"1 CLEEMAIL",$"2 EMAIL",$"3 NOM",$"4 PRENOM",$"5 ZIP",$"6 VILLE",$"7 DPT",$"8 PAYS",$"9 LANGUE",$"10 NIC",$"11 TELEPHONE",$"12 FAX",$"13 CLIENT",$"14 MARKETING",$"15 ORGANISATION",$"16 DATE_CREATION",$"17 DATE_MAJ",$"18 DATE_ENVOI_CODE",$"19 ACTIF",$"20 BILLINGCOUNTRY",$"21 RESELLER",$"22 FORKEDFROMNIC")
// On sauvegarde le dataframe
dfNicFirstFinal.write
.format("com.databricks.spark.csv")
.option("header", "true")
// Sauvegarde des refunds dans le fichier de sortie
.save("output/nic_email_unique_" + System.currentTimeMillis())

//exit and not launch scala interpretor
System.exit(0)
