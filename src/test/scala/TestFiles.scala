trait TestFiles {

  val csvFile: Iterable[String] =
    """|header1,header2
       |1,2
       |2,3
       |2,3
       |2,3
       |4,5
       |""".stripMargin.split("\n")

  val tsvFile: Iterable[String] =
    s"""headerA\theaderB
       |1\t2
       |4\t5
       |4\t5
       |3\t0
       |3\t0
       |3\t0
       |""".stripMargin.split("\n")

  val csvFileWithMissing: Iterable[String] =
    """X,Y
      |,0
      |0,
      |0,0
      |1,
      |1,
      |3,
      |""".stripMargin.split("\n")

  val tsvFileWithMissing: Iterable[String] =
    s"""apples\toranges
       |1\t
       |\t0
       |0\t
       |4\t5
       |3\t
       |""".stripMargin.split("\n")

  lazy val allFiles: Iterable[String] = csvFile ++ csvFileWithMissing ++ tsvFile ++ tsvFileWithMissing

}
