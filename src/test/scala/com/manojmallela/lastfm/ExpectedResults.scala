package com.manojmallela.lastfm

import java.sql.Timestamp

object ExpectedResults {

  val distinctSongCountExpected: Array[(String, Long)] = Array(("user_000002", 10), ("user_001000", 10), ("user_000012", 2))

  val top100SongsExpected: Array[(String, String, Int)] = Array(
    ("Echt", "Wir Haben'S Getan", 1),
    ("Wilco", "Impossible Germany", 1),
    ("Wilco", "Shake It Off", 1),
    ("Wilco", "Leave Me (Like You Found Me)", 1),
    ("Virginia Jetzt!", "Draußen Im Regen", 1),
    ("Wilco", "Please Be Patient With Me", 1),
    ("Tomte", "Die Nacht In Der Ich Starb", 1),
    ("Olli Schulz & Der Hund Marie", "Keiner Hier Bewegt Sich", 1),
    ("Tocotronic", "Mein Ruin (Live)", 1),
    ("Wilco", "Hate It Here", 1),
    ("Wir Sind Helden", "Alphamännchen", 1),
    ("Wilco", "Sky Blue Sky", 1),
    ("Wilco", "On And On And On", 1),
    ("The National", "Friend Of Mine", 1),
    ("The Tragically Hip", "Emergency", 1),
    ("Die Sterne", "Risikobiographie", 1),
    ("Astra Kid", "Jogginghose", 1),
    ("Liquido", "Fake Boys/Girls", 1),
    ("Wilco", "Walken", 1),
    ("Wilco", "Side With The Seeds", 1),
    ("...But Alive", "Antimanifest", 1),
    ("Wilco", "What Light", 1)
  )


  val top10SessionsExpected: Array[TopSession] = Array(
    TopSession("user_001000", "2008-01-27 21:43:14.0", "2008-01-27 22:22:43.0", List("Impossible Germany", "Sky Blue Sky", "Side With The Seeds", "Shake It Off", "Please Be Patient With Me", "Hate It Here", "Leave Me (Like You Found Me)", "Walken", "What Light", "On And On And On")),
    TopSession("user_000002", "2008-02-25 11:11:37.0", "2008-02-25 11:44:16.0", List("Jogginghose", "Draußen Im Regen", "Alphamännchen", "Mein Ruin (Live)", "Antimanifest", "Risikobiographie", "Die Nacht In Der Ich Starb", "Keiner Hier Bewegt Sich", "Fake Boys/Girls", "Wir Haben'S Getan")),
    TopSession("user_000012", "2005-10-23 22:57:10.0", "2005-10-23 23:00:57.0", List("Friend Of Mine", "Emergency"))
  )


  case class TopSession(userId: String, startedAt: String, endedAt: String, songsPlayed: List[String])

}
