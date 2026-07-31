package org.radiant.opendatalake.wap.iceberg

import bio.ferlab.datalake.commons.config.TableConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/*
  The pure parts only — no Spark, so this runs instantly. Everything that touches a catalog is covered
  end-to-end by WapLoaderSpec, which drives WapLoader.publish and asserts through raw SQL.
*/
class IcebergTableSpec extends AnyFlatSpec with Matchers {

  "IcebergTable" should "qualify its name with the database" in {
    IcebergTable("reference", "clinvar").fullName shouldBe "reference.clinvar"
  }

  /*
    FerLab's TableConf.fullName returns the bare table name when database is empty, which would hand Iceberg
    an unqualified identifier. Ours stays qualified — pinning the difference so nobody "simplifies" this into
    a delegation to TableConf.fullName.
  */
  it should "stay qualified even when the database is empty, unlike TableConf.fullName" in {
    TableConf("", "clinvar").fullName shouldBe "clinvar"
    IcebergTable("", "clinvar").fullName shouldBe ".clinvar"
  }

  it should "be constructible from the TableConf a DatasetConf carries" in {
    IcebergTable(TableConf("reference", "dbsnp")) shouldBe IcebergTable("reference", "dbsnp")
  }

  it should "expose main as the branch every version is cut from" in {
    IcebergTable.MainBranch shouldBe "main"
  }

  "IcebergDatabase" should "build tables in its own namespace" in {
    IcebergDatabase("reference").table("clinvar") shouldBe IcebergTable("reference", "clinvar")
    IcebergDatabase("reference").table("clinvar").fullName shouldBe "reference.clinvar"
  }
}
