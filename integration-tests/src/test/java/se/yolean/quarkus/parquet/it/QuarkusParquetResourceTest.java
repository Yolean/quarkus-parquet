package se.yolean.quarkus.parquet.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class QuarkusParquetResourceTest {

    @Test
    public void testHelloEndpoint() {
        given()
                .when().get("/quarkus-parquet")
                .then()
                .statusCode(200)
                .body(is("Hello quarkus-parquet"));
    }
}
