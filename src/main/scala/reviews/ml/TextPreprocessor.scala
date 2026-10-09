// src/main/scala/reviews/ml/TextPreprocessor.scala
package reviews.ml

import java.text.Normalizer
import java.util.Locale
import java.util.regex.Pattern

/**
 * Preprocesamiento de texto para el baseline TF-IDF.
 *
 * Procedencia: `negations` y `stopWords` están copiados sin modificación de
 * `reviews.eda.Opinions` (Opinions.scala). Allí, `stopWords` se define como la
 * lista literal menos las negaciones ("no", "nunca", "sin"), de modo que las
 * negaciones nunca se descartan.
 */
object TextPreprocessor {

  // --- Copiado de Opinions.scala (sin modificar) ---
  private val negations = Set("no", "nunca", "sin")

  private val stopWords =
    Set(
      "el", "la", "los", "las", "un", "una", "unos", "unas",
      "de", "del", "a", "al", "en", "por", "para", "con",
      "su", "sus", "y", "o", "pero", "que", "es", "son",
      "fue", "muy", "mas", "más", "lo", "se", "me", "nos",
      "este", "era", "si", "mi", "todo", "como", "esta",
      "está", "ya", "hay", "tiene", "hasta", "desde"
    ) -- negations
  // --- Fin de lo copiado ---

  // Separa por todo lo que NO sea letra Unicode, marca combinante (acentos
  // descompuestos) o dígito decimal Unicode.
  private val splitPattern: Pattern = Pattern.compile("[^\\p{L}\\p{M}\\p{Nd}]+")

  /**
   * Minúsculas (Locale.ROOT), tokenización con letras Unicode y dígitos,
   * y filtrado de stop words conservando siempre las negaciones.
   * Un texto sin tokens produce una secuencia vacía.
   */
  def tokenize(text: String): Seq[String] = {
    if (text == null) Seq.empty
    else {
      val normalized = Normalizer
        .normalize(text, Normalizer.Form.NFC)
        .toLowerCase(Locale.ROOT)

      splitPattern
        .split(normalized)
        .iterator
        .filter(_.nonEmpty)
        .filter(w => negations.contains(w) || !stopWords.contains(w))
        .toVector
    }
  }
}