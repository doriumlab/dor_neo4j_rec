package dor.neo4j

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.index.Index
import org.neo4j.graphdb.index.IndexManager
import org.neo4j.logging.Log
import org.neo4j.graphdb.*

import org.neo4j.helpers.collection.MapUtil.stringMap
import org.neo4j.graphdb.QueryExecutionType.query
import org.neo4j.procedure.*
import java.time.Year
import java.util.stream.Stream
import javax.xml.crypto.Data


class ProcessData {

    enum class Relations {
        PRODUCT_IN_RS,
    }

    @Context
    lateinit var db: GraphDatabaseService


    @Context
    lateinit var log: Log


    @Procedure(name = "adt.createProduct")
    fun createProduct() {
        val q = "Create (c:Product { product })\n" +
                "With c\n" +
                "Match (rs:RS)\n" +
                "Where rs.SiteId = \"50cfc9e8-402b-495b-8ed4-66dcb2b3aadd\"\n" +
                "Create Unique (rs)<-[:${Relations.PRODUCT_IN_RS.toString()}]-(c)"
        db.execute(q)
    }

    @Procedure(name = "adt.defineCategory")
    fun defineProductCategory(@Name("HashTitle") hashTitle: String) {
        val productLabel: Label = Label.label("Product")
        val categoryDetectLabel: Label = Label.label("CategoryDetect")

        val product: Node = db.findNode(productLabel, "HashTitle", hashTitle)

        var productContent: String = "${product.getProperty("Description")} ${product.getProperty("FaTitle")} ${product.getProperty("EnTitle")}"

        var query = "MATCH(s:SiteConfiguration)-[:CATEGORYDETECT_IN_SITE]-(cd:CategoryDetect) WHERE '$productContent' CONTAINS cd.Title RETURN cd"
        var result: Res


        log.info(result.toString())

    }


    @UserFunction(name = "adt.hello")
    fun sayHello(@Name("hello") zz: String): String {
        return "$zz hello"
    }
}

