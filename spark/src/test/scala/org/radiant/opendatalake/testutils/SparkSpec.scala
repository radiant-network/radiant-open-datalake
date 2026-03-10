package org.radiant.opendatalake.testutils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

trait SparkSpec extends AnyFlatSpec with WithSparkTestEnvironment with Matchers
