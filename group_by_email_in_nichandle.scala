/////////////////////////////////////////////////
// Nom du fichier       :  group_by_email_in_nichandle.scala
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
.option("delimiter", ",")
.option("quote", "|")
.option("header", "true")
val dfNic = dataFrameReader.load("/home/ig2i/data/n2.2/nichandle.csv")


// Création d'une instance de la calsse nic en splittant le csv avec un point virgule
// NB : _.split() <=> 'x => x.split()'
val RDDNic = CSVNic.map(_.split(",")).map(p => NicHandle(p(0).toString,p(1).toString,p(2).toString,p(3).toString,p(4).toString,p(5).toString,p(6).toString,p(7).toString,p(8).toString,p(9).toString,p(10).toString,p(11).toString,p(12).toString,p(13).toString,p(14).toString,p(15).toString,p(16).toString,p(17).toString,p(18).toString,p(19).toString,p(20).toString,p(21).toString))

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
val dfNicFirst = dfNicGroupByFirstNic.join(dfNic, dfNicGroupByFirstNic("_1") === dfNic("NIC"))
// On sélectionne toutes les colonnes qui ont été récupéré dans la jointure
val dfNicFirstFinal = dfNicFirst.select($"CLEEMAIL",$"EMAIL",$"NOM",$"PRENOM",$"ZIP",$"VILLE",$"DPT",$"PAYS",$"LANGUE",$"NIC",$"TELEPHONE",$"FAX",$"CLIENT",$"MARKETING",$"ORGANISATION",$"DATE_CREATION",$"DATE_MAJ",$"DATE_ENVOI_CODE",$"ACTIF",$"BILLINGCOUNTRY",$"RESELLER",$"FORKEDFROMNIC")
// On sauvegarde le dataframe
dfNicFirstFinal.write
.format("com.databricks.spark.csv")
.option("header", "true")
// Sauvegarde des refunds dans le fichier de sortie
.save("output/nic_email_unique_" + System.currentTimeMillis())

//exit and not launch scala interpretor
System.exit(0)
