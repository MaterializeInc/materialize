/// MySql collation type.
///
/// Collected via:
///
/// ```sql
/// SELECT CONCAT(
///     UPPER(COLLATION_NAME), ' = ', ID, ','
/// )
/// FROM INFORMATION_SCHEMA.COLLATIONS
/// ORDER BY ID;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
#[repr(u16)]
pub enum CollationId {
    /// This is a stub
    UNKNOWN_COLLATION_ID = 0,
    BIG5_CHINESE_CI = 1,
    LATIN2_CZECH_CS = 2,
    DEC8_SWEDISH_CI = 3,
    CP850_GENERAL_CI = 4,
    LATIN1_GERMAN1_CI = 5,
    HP8_ENGLISH_CI = 6,
    KOI8R_GENERAL_CI = 7,
    LATIN1_SWEDISH_CI = 8,
    LATIN2_GENERAL_CI = 9,
    SWE7_SWEDISH_CI = 10,
    ASCII_GENERAL_CI = 11,
    UJIS_JAPANESE_CI = 12,
    SJIS_JAPANESE_CI = 13,
    CP1251_BULGARIAN_CI = 14,
    LATIN1_DANISH_CI = 15,
    HEBREW_GENERAL_CI = 16,
    TIS620_THAI_CI = 18,
    EUCKR_KOREAN_CI = 19,
    LATIN7_ESTONIAN_CS = 20,
    LATIN2_HUNGARIAN_CI = 21,
    KOI8U_GENERAL_CI = 22,
    CP1251_UKRAINIAN_CI = 23,
    GB2312_CHINESE_CI = 24,
    GREEK_GENERAL_CI = 25,
    CP1250_GENERAL_CI = 26,
    LATIN2_CROATIAN_CI = 27,
    GBK_CHINESE_CI = 28,
    CP1257_LITHUANIAN_CI = 29,
    LATIN5_TURKISH_CI = 30,
    LATIN1_GERMAN2_CI = 31,
    ARMSCII8_GENERAL_CI = 32,
    UTF8MB3_GENERAL_CI = 33,
    CP1250_CZECH_CS = 34,
    UCS2_GENERAL_CI = 35,
    CP866_GENERAL_CI = 36,
    KEYBCS2_GENERAL_CI = 37,
    MACCE_GENERAL_CI = 38,
    MACROMAN_GENERAL_CI = 39,
    CP852_GENERAL_CI = 40,
    LATIN7_GENERAL_CI = 41,
    LATIN7_GENERAL_CS = 42,
    MACCE_BIN = 43,
    CP1250_CROATIAN_CI = 44,
    UTF8MB4_GENERAL_CI = 45,
    UTF8MB4_BIN = 46,
    LATIN1_BIN = 47,
    LATIN1_GENERAL_CI = 48,
    LATIN1_GENERAL_CS = 49,
    CP1251_BIN = 50,
    CP1251_GENERAL_CI = 51,
    CP1251_GENERAL_CS = 52,
    MACROMAN_BIN = 53,
    UTF16_GENERAL_CI = 54,
    UTF16_BIN = 55,
    UTF16LE_GENERAL_CI = 56,
    CP1256_GENERAL_CI = 57,
    CP1257_BIN = 58,
    CP1257_GENERAL_CI = 59,
    UTF32_GENERAL_CI = 60,
    UTF32_BIN = 61,
    UTF16LE_BIN = 62,
    BINARY = 63,
    ARMSCII8_BIN = 64,
    ASCII_BIN = 65,
    CP1250_BIN = 66,
    CP1256_BIN = 67,
    CP866_BIN = 68,
    DEC8_BIN = 69,
    GREEK_BIN = 70,
    HEBREW_BIN = 71,
    HP8_BIN = 72,
    KEYBCS2_BIN = 73,
    KOI8R_BIN = 74,
    KOI8U_BIN = 75,
    UTF8MB3_TOLOWER_CI = 76,
    LATIN2_BIN = 77,
    LATIN5_BIN = 78,
    LATIN7_BIN = 79,
    CP850_BIN = 80,
    CP852_BIN = 81,
    SWE7_BIN = 82,
    UTF8MB3_BIN = 83,
    BIG5_BIN = 84,
    EUCKR_BIN = 85,
    GB2312_BIN = 86,
    GBK_BIN = 87,
    SJIS_BIN = 88,
    TIS620_BIN = 89,
    UCS2_BIN = 90,
    UJIS_BIN = 91,
    GEOSTD8_GENERAL_CI = 92,
    GEOSTD8_BIN = 93,
    LATIN1_SPANISH_CI = 94,
    CP932_JAPANESE_CI = 95,
    CP932_BIN = 96,
    EUCJPMS_JAPANESE_CI = 97,
    EUCJPMS_BIN = 98,
    CP1250_POLISH_CI = 99,
    UTF16_UNICODE_CI = 101,
    UTF16_ICELANDIC_CI = 102,
    UTF16_LATVIAN_CI = 103,
    UTF16_ROMANIAN_CI = 104,
    UTF16_SLOVENIAN_CI = 105,
    UTF16_POLISH_CI = 106,
    UTF16_ESTONIAN_CI = 107,
    UTF16_SPANISH_CI = 108,
    UTF16_SWEDISH_CI = 109,
    UTF16_TURKISH_CI = 110,
    UTF16_CZECH_CI = 111,
    UTF16_DANISH_CI = 112,
    UTF16_LITHUANIAN_CI = 113,
    UTF16_SLOVAK_CI = 114,
    UTF16_SPANISH2_CI = 115,
    UTF16_ROMAN_CI = 116,
    UTF16_PERSIAN_CI = 117,
    UTF16_ESPERANTO_CI = 118,
    UTF16_HUNGARIAN_CI = 119,
    UTF16_SINHALA_CI = 120,
    UTF16_GERMAN2_CI = 121,
    UTF16_CROATIAN_CI = 122,
    UTF16_UNICODE_520_CI = 123,
    UTF16_VIETNAMESE_CI = 124,
    UCS2_UNICODE_CI = 128,
    UCS2_ICELANDIC_CI = 129,
    UCS2_LATVIAN_CI = 130,
    UCS2_ROMANIAN_CI = 131,
    UCS2_SLOVENIAN_CI = 132,
    UCS2_POLISH_CI = 133,
    UCS2_ESTONIAN_CI = 134,
    UCS2_SPANISH_CI = 135,
    UCS2_SWEDISH_CI = 136,
    UCS2_TURKISH_CI = 137,
    UCS2_CZECH_CI = 138,
    UCS2_DANISH_CI = 139,
    UCS2_LITHUANIAN_CI = 140,
    UCS2_SLOVAK_CI = 141,
    UCS2_SPANISH2_CI = 142,
    UCS2_ROMAN_CI = 143,
    UCS2_PERSIAN_CI = 144,
    UCS2_ESPERANTO_CI = 145,
    UCS2_HUNGARIAN_CI = 146,
    UCS2_SINHALA_CI = 147,
    UCS2_GERMAN2_CI = 148,
    UCS2_CROATIAN_CI = 149,
    UCS2_UNICODE_520_CI = 150,
    UCS2_VIETNAMESE_CI = 151,
    UCS2_GENERAL_MYSQL500_CI = 159,
    UTF32_UNICODE_CI = 160,
    UTF32_ICELANDIC_CI = 161,
    UTF32_LATVIAN_CI = 162,
    UTF32_ROMANIAN_CI = 163,
    UTF32_SLOVENIAN_CI = 164,
    UTF32_POLISH_CI = 165,
    UTF32_ESTONIAN_CI = 166,
    UTF32_SPANISH_CI = 167,
    UTF32_SWEDISH_CI = 168,
    UTF32_TURKISH_CI = 169,
    UTF32_CZECH_CI = 170,
    UTF32_DANISH_CI = 171,
    UTF32_LITHUANIAN_CI = 172,
    UTF32_SLOVAK_CI = 173,
    UTF32_SPANISH2_CI = 174,
    UTF32_ROMAN_CI = 175,
    UTF32_PERSIAN_CI = 176,
    UTF32_ESPERANTO_CI = 177,
    UTF32_HUNGARIAN_CI = 178,
    UTF32_SINHALA_CI = 179,
    UTF32_GERMAN2_CI = 180,
    UTF32_CROATIAN_CI = 181,
    UTF32_UNICODE_520_CI = 182,
    UTF32_VIETNAMESE_CI = 183,
    UTF8MB3_UNICODE_CI = 192,
    UTF8MB3_ICELANDIC_CI = 193,
    UTF8MB3_LATVIAN_CI = 194,
    UTF8MB3_ROMANIAN_CI = 195,
    UTF8MB3_SLOVENIAN_CI = 196,
    UTF8MB3_POLISH_CI = 197,
    UTF8MB3_ESTONIAN_CI = 198,
    UTF8MB3_SPANISH_CI = 199,
    UTF8MB3_SWEDISH_CI = 200,
    UTF8MB3_TURKISH_CI = 201,
    UTF8MB3_CZECH_CI = 202,
    UTF8MB3_DANISH_CI = 203,
    UTF8MB3_LITHUANIAN_CI = 204,
    UTF8MB3_SLOVAK_CI = 205,
    UTF8MB3_SPANISH2_CI = 206,
    UTF8MB3_ROMAN_CI = 207,
    UTF8MB3_PERSIAN_CI = 208,
    UTF8MB3_ESPERANTO_CI = 209,
    UTF8MB3_HUNGARIAN_CI = 210,
    UTF8MB3_SINHALA_CI = 211,
    UTF8MB3_GERMAN2_CI = 212,
    UTF8MB3_CROATIAN_CI = 213,
    UTF8MB3_UNICODE_520_CI = 214,
    UTF8MB3_VIETNAMESE_CI = 215,
    UTF8MB3_GENERAL_MYSQL500_CI = 223,
    UTF8MB4_UNICODE_CI = 224,
    UTF8MB4_ICELANDIC_CI = 225,
    UTF8MB4_LATVIAN_CI = 226,
    UTF8MB4_ROMANIAN_CI = 227,
    UTF8MB4_SLOVENIAN_CI = 228,
    UTF8MB4_POLISH_CI = 229,
    UTF8MB4_ESTONIAN_CI = 230,
    UTF8MB4_SPANISH_CI = 231,
    UTF8MB4_SWEDISH_CI = 232,
    UTF8MB4_TURKISH_CI = 233,
    UTF8MB4_CZECH_CI = 234,
    UTF8MB4_DANISH_CI = 235,
    UTF8MB4_LITHUANIAN_CI = 236,
    UTF8MB4_SLOVAK_CI = 237,
    UTF8MB4_SPANISH2_CI = 238,
    UTF8MB4_ROMAN_CI = 239,
    UTF8MB4_PERSIAN_CI = 240,
    UTF8MB4_ESPERANTO_CI = 241,
    UTF8MB4_HUNGARIAN_CI = 242,
    UTF8MB4_SINHALA_CI = 243,
    UTF8MB4_GERMAN2_CI = 244,
    UTF8MB4_CROATIAN_CI = 245,
    UTF8MB4_UNICODE_520_CI = 246,
    UTF8MB4_VIETNAMESE_CI = 247,
    GB18030_CHINESE_CI = 248,
    GB18030_BIN = 249,
    GB18030_UNICODE_520_CI = 250,
    UTF8MB4_0900_AI_CI = 255,
    UTF8MB4_DE_PB_0900_AI_CI = 256,
    UTF8MB4_IS_0900_AI_CI = 257,
    UTF8MB4_LV_0900_AI_CI = 258,
    UTF8MB4_RO_0900_AI_CI = 259,
    UTF8MB4_SL_0900_AI_CI = 260,
    UTF8MB4_PL_0900_AI_CI = 261,
    UTF8MB4_ET_0900_AI_CI = 262,
    UTF8MB4_ES_0900_AI_CI = 263,
    UTF8MB4_SV_0900_AI_CI = 264,
    UTF8MB4_TR_0900_AI_CI = 265,
    UTF8MB4_CS_0900_AI_CI = 266,
    UTF8MB4_DA_0900_AI_CI = 267,
    UTF8MB4_LT_0900_AI_CI = 268,
    UTF8MB4_SK_0900_AI_CI = 269,
    UTF8MB4_ES_TRAD_0900_AI_CI = 270,
    UTF8MB4_LA_0900_AI_CI = 271,
    UTF8MB4_EO_0900_AI_CI = 273,
    UTF8MB4_HU_0900_AI_CI = 274,
    UTF8MB4_HR_0900_AI_CI = 275,
    UTF8MB4_VI_0900_AI_CI = 277,
    UTF8MB4_0900_AS_CS = 278,
    UTF8MB4_DE_PB_0900_AS_CS = 279,
    UTF8MB4_IS_0900_AS_CS = 280,
    UTF8MB4_LV_0900_AS_CS = 281,
    UTF8MB4_RO_0900_AS_CS = 282,
    UTF8MB4_SL_0900_AS_CS = 283,
    UTF8MB4_PL_0900_AS_CS = 284,
    UTF8MB4_ET_0900_AS_CS = 285,
    UTF8MB4_ES_0900_AS_CS = 286,
    UTF8MB4_SV_0900_AS_CS = 287,
    UTF8MB4_TR_0900_AS_CS = 288,
    UTF8MB4_CS_0900_AS_CS = 289,
    UTF8MB4_DA_0900_AS_CS = 290,
    UTF8MB4_LT_0900_AS_CS = 291,
    UTF8MB4_SK_0900_AS_CS = 292,
    UTF8MB4_ES_TRAD_0900_AS_CS = 293,
    UTF8MB4_LA_0900_AS_CS = 294,
    UTF8MB4_EO_0900_AS_CS = 296,
    UTF8MB4_HU_0900_AS_CS = 297,
    UTF8MB4_HR_0900_AS_CS = 298,
    UTF8MB4_VI_0900_AS_CS = 300,
    UTF8MB4_JA_0900_AS_CS = 303,
    UTF8MB4_JA_0900_AS_CS_KS = 304,
    UTF8MB4_0900_AS_CI = 305,
    UTF8MB4_RU_0900_AI_CI = 306,
    UTF8MB4_RU_0900_AS_CS = 307,
    UTF8MB4_ZH_0900_AS_CS = 308,
    UTF8MB4_0900_BIN = 309,
    UTF8MB4_NB_0900_AI_CI = 310,
    UTF8MB4_NB_0900_AS_CS = 311,
    UTF8MB4_NN_0900_AI_CI = 312,
    UTF8MB4_NN_0900_AS_CS = 313,
    UTF8MB4_SR_LATN_0900_AI_CI = 314,
    UTF8MB4_SR_LATN_0900_AS_CS = 315,
    UTF8MB4_BS_0900_AI_CI = 316,
    UTF8MB4_BS_0900_AS_CS = 317,
    UTF8MB4_BG_0900_AI_CI = 318,
    UTF8MB4_BG_0900_AS_CS = 319,
    UTF8MB4_GL_0900_AI_CI = 320,
    UTF8MB4_GL_0900_AS_CS = 321,
    UTF8MB4_MN_CYRL_0900_AI_CI = 322,
    UTF8MB4_MN_CYRL_0900_AS_CS = 323,
}

impl From<u16> for CollationId {
    /// u16 conversion.
    ///
    /// Unknown IDs will be mapped to [`CollationId::UNKNOWN_COLLATION_ID`].
    ///
    /// Collected via:
    ///
    /// ```sql
    /// SELECT CONCAT(
    ///     ID, ' => CollationId::', UPPER(COLLATION_NAME), ','
    /// )
    /// FROM INFORMATION_SCHEMA.COLLATIONS
    /// ORDER BY ID;
    /// ```
    fn from(value: u16) -> Self {
        match value {
            1 => CollationId::BIG5_CHINESE_CI,
            2 => CollationId::LATIN2_CZECH_CS,
            3 => CollationId::DEC8_SWEDISH_CI,
            4 => CollationId::CP850_GENERAL_CI,
            5 => CollationId::LATIN1_GERMAN1_CI,
            6 => CollationId::HP8_ENGLISH_CI,
            7 => CollationId::KOI8R_GENERAL_CI,
            8 => CollationId::LATIN1_SWEDISH_CI,
            9 => CollationId::LATIN2_GENERAL_CI,
            10 => CollationId::SWE7_SWEDISH_CI,
            11 => CollationId::ASCII_GENERAL_CI,
            12 => CollationId::UJIS_JAPANESE_CI,
            13 => CollationId::SJIS_JAPANESE_CI,
            14 => CollationId::CP1251_BULGARIAN_CI,
            15 => CollationId::LATIN1_DANISH_CI,
            16 => CollationId::HEBREW_GENERAL_CI,
            18 => CollationId::TIS620_THAI_CI,
            19 => CollationId::EUCKR_KOREAN_CI,
            20 => CollationId::LATIN7_ESTONIAN_CS,
            21 => CollationId::LATIN2_HUNGARIAN_CI,
            22 => CollationId::KOI8U_GENERAL_CI,
            23 => CollationId::CP1251_UKRAINIAN_CI,
            24 => CollationId::GB2312_CHINESE_CI,
            25 => CollationId::GREEK_GENERAL_CI,
            26 => CollationId::CP1250_GENERAL_CI,
            27 => CollationId::LATIN2_CROATIAN_CI,
            28 => CollationId::GBK_CHINESE_CI,
            29 => CollationId::CP1257_LITHUANIAN_CI,
            30 => CollationId::LATIN5_TURKISH_CI,
            31 => CollationId::LATIN1_GERMAN2_CI,
            32 => CollationId::ARMSCII8_GENERAL_CI,
            33 => CollationId::UTF8MB3_GENERAL_CI,
            34 => CollationId::CP1250_CZECH_CS,
            35 => CollationId::UCS2_GENERAL_CI,
            36 => CollationId::CP866_GENERAL_CI,
            37 => CollationId::KEYBCS2_GENERAL_CI,
            38 => CollationId::MACCE_GENERAL_CI,
            39 => CollationId::MACROMAN_GENERAL_CI,
            40 => CollationId::CP852_GENERAL_CI,
            41 => CollationId::LATIN7_GENERAL_CI,
            42 => CollationId::LATIN7_GENERAL_CS,
            43 => CollationId::MACCE_BIN,
            44 => CollationId::CP1250_CROATIAN_CI,
            45 => CollationId::UTF8MB4_GENERAL_CI,
            46 => CollationId::UTF8MB4_BIN,
            47 => CollationId::LATIN1_BIN,
            48 => CollationId::LATIN1_GENERAL_CI,
            49 => CollationId::LATIN1_GENERAL_CS,
            50 => CollationId::CP1251_BIN,
            51 => CollationId::CP1251_GENERAL_CI,
            52 => CollationId::CP1251_GENERAL_CS,
            53 => CollationId::MACROMAN_BIN,
            54 => CollationId::UTF16_GENERAL_CI,
            55 => CollationId::UTF16_BIN,
            56 => CollationId::UTF16LE_GENERAL_CI,
            57 => CollationId::CP1256_GENERAL_CI,
            58 => CollationId::CP1257_BIN,
            59 => CollationId::CP1257_GENERAL_CI,
            60 => CollationId::UTF32_GENERAL_CI,
            61 => CollationId::UTF32_BIN,
            62 => CollationId::UTF16LE_BIN,
            63 => CollationId::BINARY,
            64 => CollationId::ARMSCII8_BIN,
            65 => CollationId::ASCII_BIN,
            66 => CollationId::CP1250_BIN,
            67 => CollationId::CP1256_BIN,
            68 => CollationId::CP866_BIN,
            69 => CollationId::DEC8_BIN,
            70 => CollationId::GREEK_BIN,
            71 => CollationId::HEBREW_BIN,
            72 => CollationId::HP8_BIN,
            73 => CollationId::KEYBCS2_BIN,
            74 => CollationId::KOI8R_BIN,
            75 => CollationId::KOI8U_BIN,
            76 => CollationId::UTF8MB3_TOLOWER_CI,
            77 => CollationId::LATIN2_BIN,
            78 => CollationId::LATIN5_BIN,
            79 => CollationId::LATIN7_BIN,
            80 => CollationId::CP850_BIN,
            81 => CollationId::CP852_BIN,
            82 => CollationId::SWE7_BIN,
            83 => CollationId::UTF8MB3_BIN,
            84 => CollationId::BIG5_BIN,
            85 => CollationId::EUCKR_BIN,
            86 => CollationId::GB2312_BIN,
            87 => CollationId::GBK_BIN,
            88 => CollationId::SJIS_BIN,
            89 => CollationId::TIS620_BIN,
            90 => CollationId::UCS2_BIN,
            91 => CollationId::UJIS_BIN,
            92 => CollationId::GEOSTD8_GENERAL_CI,
            93 => CollationId::GEOSTD8_BIN,
            94 => CollationId::LATIN1_SPANISH_CI,
            95 => CollationId::CP932_JAPANESE_CI,
            96 => CollationId::CP932_BIN,
            97 => CollationId::EUCJPMS_JAPANESE_CI,
            98 => CollationId::EUCJPMS_BIN,
            99 => CollationId::CP1250_POLISH_CI,
            101 => CollationId::UTF16_UNICODE_CI,
            102 => CollationId::UTF16_ICELANDIC_CI,
            103 => CollationId::UTF16_LATVIAN_CI,
            104 => CollationId::UTF16_ROMANIAN_CI,
            105 => CollationId::UTF16_SLOVENIAN_CI,
            106 => CollationId::UTF16_POLISH_CI,
            107 => CollationId::UTF16_ESTONIAN_CI,
            108 => CollationId::UTF16_SPANISH_CI,
            109 => CollationId::UTF16_SWEDISH_CI,
            110 => CollationId::UTF16_TURKISH_CI,
            111 => CollationId::UTF16_CZECH_CI,
            112 => CollationId::UTF16_DANISH_CI,
            113 => CollationId::UTF16_LITHUANIAN_CI,
            114 => CollationId::UTF16_SLOVAK_CI,
            115 => CollationId::UTF16_SPANISH2_CI,
            116 => CollationId::UTF16_ROMAN_CI,
            117 => CollationId::UTF16_PERSIAN_CI,
            118 => CollationId::UTF16_ESPERANTO_CI,
            119 => CollationId::UTF16_HUNGARIAN_CI,
            120 => CollationId::UTF16_SINHALA_CI,
            121 => CollationId::UTF16_GERMAN2_CI,
            122 => CollationId::UTF16_CROATIAN_CI,
            123 => CollationId::UTF16_UNICODE_520_CI,
            124 => CollationId::UTF16_VIETNAMESE_CI,
            128 => CollationId::UCS2_UNICODE_CI,
            129 => CollationId::UCS2_ICELANDIC_CI,
            130 => CollationId::UCS2_LATVIAN_CI,
            131 => CollationId::UCS2_ROMANIAN_CI,
            132 => CollationId::UCS2_SLOVENIAN_CI,
            133 => CollationId::UCS2_POLISH_CI,
            134 => CollationId::UCS2_ESTONIAN_CI,
            135 => CollationId::UCS2_SPANISH_CI,
            136 => CollationId::UCS2_SWEDISH_CI,
            137 => CollationId::UCS2_TURKISH_CI,
            138 => CollationId::UCS2_CZECH_CI,
            139 => CollationId::UCS2_DANISH_CI,
            140 => CollationId::UCS2_LITHUANIAN_CI,
            141 => CollationId::UCS2_SLOVAK_CI,
            142 => CollationId::UCS2_SPANISH2_CI,
            143 => CollationId::UCS2_ROMAN_CI,
            144 => CollationId::UCS2_PERSIAN_CI,
            145 => CollationId::UCS2_ESPERANTO_CI,
            146 => CollationId::UCS2_HUNGARIAN_CI,
            147 => CollationId::UCS2_SINHALA_CI,
            148 => CollationId::UCS2_GERMAN2_CI,
            149 => CollationId::UCS2_CROATIAN_CI,
            150 => CollationId::UCS2_UNICODE_520_CI,
            151 => CollationId::UCS2_VIETNAMESE_CI,
            159 => CollationId::UCS2_GENERAL_MYSQL500_CI,
            160 => CollationId::UTF32_UNICODE_CI,
            161 => CollationId::UTF32_ICELANDIC_CI,
            162 => CollationId::UTF32_LATVIAN_CI,
            163 => CollationId::UTF32_ROMANIAN_CI,
            164 => CollationId::UTF32_SLOVENIAN_CI,
            165 => CollationId::UTF32_POLISH_CI,
            166 => CollationId::UTF32_ESTONIAN_CI,
            167 => CollationId::UTF32_SPANISH_CI,
            168 => CollationId::UTF32_SWEDISH_CI,
            169 => CollationId::UTF32_TURKISH_CI,
            170 => CollationId::UTF32_CZECH_CI,
            171 => CollationId::UTF32_DANISH_CI,
            172 => CollationId::UTF32_LITHUANIAN_CI,
            173 => CollationId::UTF32_SLOVAK_CI,
            174 => CollationId::UTF32_SPANISH2_CI,
            175 => CollationId::UTF32_ROMAN_CI,
            176 => CollationId::UTF32_PERSIAN_CI,
            177 => CollationId::UTF32_ESPERANTO_CI,
            178 => CollationId::UTF32_HUNGARIAN_CI,
            179 => CollationId::UTF32_SINHALA_CI,
            180 => CollationId::UTF32_GERMAN2_CI,
            181 => CollationId::UTF32_CROATIAN_CI,
            182 => CollationId::UTF32_UNICODE_520_CI,
            183 => CollationId::UTF32_VIETNAMESE_CI,
            192 => CollationId::UTF8MB3_UNICODE_CI,
            193 => CollationId::UTF8MB3_ICELANDIC_CI,
            194 => CollationId::UTF8MB3_LATVIAN_CI,
            195 => CollationId::UTF8MB3_ROMANIAN_CI,
            196 => CollationId::UTF8MB3_SLOVENIAN_CI,
            197 => CollationId::UTF8MB3_POLISH_CI,
            198 => CollationId::UTF8MB3_ESTONIAN_CI,
            199 => CollationId::UTF8MB3_SPANISH_CI,
            200 => CollationId::UTF8MB3_SWEDISH_CI,
            201 => CollationId::UTF8MB3_TURKISH_CI,
            202 => CollationId::UTF8MB3_CZECH_CI,
            203 => CollationId::UTF8MB3_DANISH_CI,
            204 => CollationId::UTF8MB3_LITHUANIAN_CI,
            205 => CollationId::UTF8MB3_SLOVAK_CI,
            206 => CollationId::UTF8MB3_SPANISH2_CI,
            207 => CollationId::UTF8MB3_ROMAN_CI,
            208 => CollationId::UTF8MB3_PERSIAN_CI,
            209 => CollationId::UTF8MB3_ESPERANTO_CI,
            210 => CollationId::UTF8MB3_HUNGARIAN_CI,
            211 => CollationId::UTF8MB3_SINHALA_CI,
            212 => CollationId::UTF8MB3_GERMAN2_CI,
            213 => CollationId::UTF8MB3_CROATIAN_CI,
            214 => CollationId::UTF8MB3_UNICODE_520_CI,
            215 => CollationId::UTF8MB3_VIETNAMESE_CI,
            223 => CollationId::UTF8MB3_GENERAL_MYSQL500_CI,
            224 => CollationId::UTF8MB4_UNICODE_CI,
            225 => CollationId::UTF8MB4_ICELANDIC_CI,
            226 => CollationId::UTF8MB4_LATVIAN_CI,
            227 => CollationId::UTF8MB4_ROMANIAN_CI,
            228 => CollationId::UTF8MB4_SLOVENIAN_CI,
            229 => CollationId::UTF8MB4_POLISH_CI,
            230 => CollationId::UTF8MB4_ESTONIAN_CI,
            231 => CollationId::UTF8MB4_SPANISH_CI,
            232 => CollationId::UTF8MB4_SWEDISH_CI,
            233 => CollationId::UTF8MB4_TURKISH_CI,
            234 => CollationId::UTF8MB4_CZECH_CI,
            235 => CollationId::UTF8MB4_DANISH_CI,
            236 => CollationId::UTF8MB4_LITHUANIAN_CI,
            237 => CollationId::UTF8MB4_SLOVAK_CI,
            238 => CollationId::UTF8MB4_SPANISH2_CI,
            239 => CollationId::UTF8MB4_ROMAN_CI,
            240 => CollationId::UTF8MB4_PERSIAN_CI,
            241 => CollationId::UTF8MB4_ESPERANTO_CI,
            242 => CollationId::UTF8MB4_HUNGARIAN_CI,
            243 => CollationId::UTF8MB4_SINHALA_CI,
            244 => CollationId::UTF8MB4_GERMAN2_CI,
            245 => CollationId::UTF8MB4_CROATIAN_CI,
            246 => CollationId::UTF8MB4_UNICODE_520_CI,
            247 => CollationId::UTF8MB4_VIETNAMESE_CI,
            248 => CollationId::GB18030_CHINESE_CI,
            249 => CollationId::GB18030_BIN,
            250 => CollationId::GB18030_UNICODE_520_CI,
            255 => CollationId::UTF8MB4_0900_AI_CI,
            256 => CollationId::UTF8MB4_DE_PB_0900_AI_CI,
            257 => CollationId::UTF8MB4_IS_0900_AI_CI,
            258 => CollationId::UTF8MB4_LV_0900_AI_CI,
            259 => CollationId::UTF8MB4_RO_0900_AI_CI,
            260 => CollationId::UTF8MB4_SL_0900_AI_CI,
            261 => CollationId::UTF8MB4_PL_0900_AI_CI,
            262 => CollationId::UTF8MB4_ET_0900_AI_CI,
            263 => CollationId::UTF8MB4_ES_0900_AI_CI,
            264 => CollationId::UTF8MB4_SV_0900_AI_CI,
            265 => CollationId::UTF8MB4_TR_0900_AI_CI,
            266 => CollationId::UTF8MB4_CS_0900_AI_CI,
            267 => CollationId::UTF8MB4_DA_0900_AI_CI,
            268 => CollationId::UTF8MB4_LT_0900_AI_CI,
            269 => CollationId::UTF8MB4_SK_0900_AI_CI,
            270 => CollationId::UTF8MB4_ES_TRAD_0900_AI_CI,
            271 => CollationId::UTF8MB4_LA_0900_AI_CI,
            273 => CollationId::UTF8MB4_EO_0900_AI_CI,
            274 => CollationId::UTF8MB4_HU_0900_AI_CI,
            275 => CollationId::UTF8MB4_HR_0900_AI_CI,
            277 => CollationId::UTF8MB4_VI_0900_AI_CI,
            278 => CollationId::UTF8MB4_0900_AS_CS,
            279 => CollationId::UTF8MB4_DE_PB_0900_AS_CS,
            280 => CollationId::UTF8MB4_IS_0900_AS_CS,
            281 => CollationId::UTF8MB4_LV_0900_AS_CS,
            282 => CollationId::UTF8MB4_RO_0900_AS_CS,
            283 => CollationId::UTF8MB4_SL_0900_AS_CS,
            284 => CollationId::UTF8MB4_PL_0900_AS_CS,
            285 => CollationId::UTF8MB4_ET_0900_AS_CS,
            286 => CollationId::UTF8MB4_ES_0900_AS_CS,
            287 => CollationId::UTF8MB4_SV_0900_AS_CS,
            288 => CollationId::UTF8MB4_TR_0900_AS_CS,
            289 => CollationId::UTF8MB4_CS_0900_AS_CS,
            290 => CollationId::UTF8MB4_DA_0900_AS_CS,
            291 => CollationId::UTF8MB4_LT_0900_AS_CS,
            292 => CollationId::UTF8MB4_SK_0900_AS_CS,
            293 => CollationId::UTF8MB4_ES_TRAD_0900_AS_CS,
            294 => CollationId::UTF8MB4_LA_0900_AS_CS,
            296 => CollationId::UTF8MB4_EO_0900_AS_CS,
            297 => CollationId::UTF8MB4_HU_0900_AS_CS,
            298 => CollationId::UTF8MB4_HR_0900_AS_CS,
            300 => CollationId::UTF8MB4_VI_0900_AS_CS,
            303 => CollationId::UTF8MB4_JA_0900_AS_CS,
            304 => CollationId::UTF8MB4_JA_0900_AS_CS_KS,
            305 => CollationId::UTF8MB4_0900_AS_CI,
            306 => CollationId::UTF8MB4_RU_0900_AI_CI,
            307 => CollationId::UTF8MB4_RU_0900_AS_CS,
            308 => CollationId::UTF8MB4_ZH_0900_AS_CS,
            309 => CollationId::UTF8MB4_0900_BIN,
            310 => CollationId::UTF8MB4_NB_0900_AI_CI,
            311 => CollationId::UTF8MB4_NB_0900_AS_CS,
            312 => CollationId::UTF8MB4_NN_0900_AI_CI,
            313 => CollationId::UTF8MB4_NN_0900_AS_CS,
            314 => CollationId::UTF8MB4_SR_LATN_0900_AI_CI,
            315 => CollationId::UTF8MB4_SR_LATN_0900_AS_CS,
            316 => CollationId::UTF8MB4_BS_0900_AI_CI,
            317 => CollationId::UTF8MB4_BS_0900_AS_CS,
            318 => CollationId::UTF8MB4_BG_0900_AI_CI,
            319 => CollationId::UTF8MB4_BG_0900_AS_CS,
            320 => CollationId::UTF8MB4_GL_0900_AI_CI,
            321 => CollationId::UTF8MB4_GL_0900_AS_CS,
            322 => CollationId::UTF8MB4_MN_CYRL_0900_AI_CI,
            323 => CollationId::UTF8MB4_MN_CYRL_0900_AS_CS,
            _ => CollationId::UNKNOWN_COLLATION_ID,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PadAttribute {
    PadZero,
    PadSpace,
}

/// MySQL server collation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Collation<'a> {
    pub id: CollationId,
    pub charset: &'a str,
    pub collation: &'a str,
    pub is_default: bool,
    pub padding: PadAttribute,
    pub is_compiled: bool,
    pub sort_len: u8,
    pub max_len: u8,
}

/// Constants are generated using the following statement:
///
/// ```sql
/// SELECT CONCAT(
///     0x5c5c5c20, DESCRIPTION, 0x0a,
///     'const ', UPPER(COLLATION_NAME), '_COLLATION: Collation<''static> = Collation {',
///     'id: CollationId::', UPPER(COLLATION_NAME), ',',
///     'charset: "', CHARACTER_SET_NAME, '",',
///     'collation: "', COLLATION_NAME, '",',
///     'is_default: ', IF(IS_DEFAULT = 'Yes', 'true', 'false'), ',',
///     'padding: PadAttribute::', IF(PAD_ATTRIBUTE = 'PAD SPACE', 'PadSpace', 'PadZero'), ',',
///     'is_compiled: ', IF(IS_COMPILED = 'Yes', 'true', 'false'), ',',
///     'sort_len: ', SORTLEN, ',',
///     'max_len: ', MAXLEN,
/// '};')
/// FROM INFORMATION_SCHEMA.COLLATIONS
/// JOIN INFORMATION_SCHEMA.CHARACTER_SETS
/// USING(CHARACTER_SET_NAME) ORDER BY ID;
/// ```
impl<'a> Collation<'a> {
    /// This is a stub.
    const UNKNOWN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UNKNOWN_COLLATION_ID,
        charset: "unknown",
        collation: "unknown",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: false,
        sort_len: 0,
        max_len: 0,
    };
    /// Big5 Traditional Chinese
    const BIG5_CHINESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::BIG5_CHINESE_CI,
        charset: "big5",
        collation: "big5_chinese_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// ISO 8859-2 Central European
    const LATIN2_CZECH_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN2_CZECH_CS,
        charset: "latin2",
        collation: "latin2_czech_cs",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 4,
        max_len: 1,
    };
    /// DEC West European
    const DEC8_SWEDISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::DEC8_SWEDISH_CI,
        charset: "dec8",
        collation: "dec8_swedish_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// DOS West European
    const CP850_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP850_GENERAL_CI,
        charset: "cp850",
        collation: "cp850_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// cp1252 West European
    const LATIN1_GERMAN1_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN1_GERMAN1_CI,
        charset: "latin1",
        collation: "latin1_german1_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// HP West European
    const HP8_ENGLISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::HP8_ENGLISH_CI,
        charset: "hp8",
        collation: "hp8_english_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// KOI8-R Relcom Russian
    const KOI8R_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::KOI8R_GENERAL_CI,
        charset: "koi8r",
        collation: "koi8r_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// cp1252 West European
    const LATIN1_SWEDISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN1_SWEDISH_CI,
        charset: "latin1",
        collation: "latin1_swedish_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-2 Central European
    const LATIN2_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN2_GENERAL_CI,
        charset: "latin2",
        collation: "latin2_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// 7bit Swedish
    const SWE7_SWEDISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::SWE7_SWEDISH_CI,
        charset: "swe7",
        collation: "swe7_swedish_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// US ASCII
    const ASCII_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::ASCII_GENERAL_CI,
        charset: "ascii",
        collation: "ascii_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// EUC-JP Japanese
    const UJIS_JAPANESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UJIS_JAPANESE_CI,
        charset: "ujis",
        collation: "ujis_japanese_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 3,
    };
    /// Shift-JIS Japanese
    const SJIS_JAPANESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::SJIS_JAPANESE_CI,
        charset: "sjis",
        collation: "sjis_japanese_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// Windows Cyrillic
    const CP1251_BULGARIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1251_BULGARIAN_CI,
        charset: "cp1251",
        collation: "cp1251_bulgarian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// cp1252 West European
    const LATIN1_DANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN1_DANISH_CI,
        charset: "latin1",
        collation: "latin1_danish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-8 Hebrew
    const HEBREW_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::HEBREW_GENERAL_CI,
        charset: "hebrew",
        collation: "hebrew_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// TIS620 Thai
    const TIS620_THAI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::TIS620_THAI_CI,
        charset: "tis620",
        collation: "tis620_thai_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 4,
        max_len: 1,
    };
    /// EUC-KR Korean
    const EUCKR_KOREAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::EUCKR_KOREAN_CI,
        charset: "euckr",
        collation: "euckr_korean_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// ISO 8859-13 Baltic
    const LATIN7_ESTONIAN_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN7_ESTONIAN_CS,
        charset: "latin7",
        collation: "latin7_estonian_cs",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-2 Central European
    const LATIN2_HUNGARIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN2_HUNGARIAN_CI,
        charset: "latin2",
        collation: "latin2_hungarian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// KOI8-U Ukrainian
    const KOI8U_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::KOI8U_GENERAL_CI,
        charset: "koi8u",
        collation: "koi8u_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Cyrillic
    const CP1251_UKRAINIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1251_UKRAINIAN_CI,
        charset: "cp1251",
        collation: "cp1251_ukrainian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// GB2312 Simplified Chinese
    const GB2312_CHINESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::GB2312_CHINESE_CI,
        charset: "gb2312",
        collation: "gb2312_chinese_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// ISO 8859-7 Greek
    const GREEK_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::GREEK_GENERAL_CI,
        charset: "greek",
        collation: "greek_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Central European
    const CP1250_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1250_GENERAL_CI,
        charset: "cp1250",
        collation: "cp1250_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-2 Central European
    const LATIN2_CROATIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN2_CROATIAN_CI,
        charset: "latin2",
        collation: "latin2_croatian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// GBK Simplified Chinese
    const GBK_CHINESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::GBK_CHINESE_CI,
        charset: "gbk",
        collation: "gbk_chinese_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// Windows Baltic
    const CP1257_LITHUANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1257_LITHUANIAN_CI,
        charset: "cp1257",
        collation: "cp1257_lithuanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-9 Turkish
    const LATIN5_TURKISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN5_TURKISH_CI,
        charset: "latin5",
        collation: "latin5_turkish_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// cp1252 West European
    const LATIN1_GERMAN2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN1_GERMAN2_CI,
        charset: "latin1",
        collation: "latin1_german2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 2,
        max_len: 1,
    };
    /// ARMSCII-8 Armenian
    const ARMSCII8_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::ARMSCII8_GENERAL_CI,
        charset: "armscii8",
        collation: "armscii8_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// UTF-8 Unicode
    const UTF8MB3_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_GENERAL_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 3,
    };
    /// Windows Central European
    const CP1250_CZECH_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1250_CZECH_CS,
        charset: "cp1250",
        collation: "cp1250_czech_cs",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 2,
        max_len: 1,
    };
    /// UCS-2 Unicode
    const UCS2_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_GENERAL_CI,
        charset: "ucs2",
        collation: "ucs2_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// DOS Russian
    const CP866_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP866_GENERAL_CI,
        charset: "cp866",
        collation: "cp866_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// DOS Kamenicky Czech-Slovak
    const KEYBCS2_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::KEYBCS2_GENERAL_CI,
        charset: "keybcs2",
        collation: "keybcs2_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Mac Central European
    const MACCE_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::MACCE_GENERAL_CI,
        charset: "macce",
        collation: "macce_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Mac West European
    const MACROMAN_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::MACROMAN_GENERAL_CI,
        charset: "macroman",
        collation: "macroman_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// DOS Central European
    const CP852_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP852_GENERAL_CI,
        charset: "cp852",
        collation: "cp852_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-13 Baltic
    const LATIN7_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN7_GENERAL_CI,
        charset: "latin7",
        collation: "latin7_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-13 Baltic
    const LATIN7_GENERAL_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN7_GENERAL_CS,
        charset: "latin7",
        collation: "latin7_general_cs",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Mac Central European
    const MACCE_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::MACCE_BIN,
        charset: "macce",
        collation: "macce_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Central European
    const CP1250_CROATIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1250_CROATIAN_CI,
        charset: "cp1250",
        collation: "cp1250_croatian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// UTF-8 Unicode
    const UTF8MB4_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_GENERAL_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_general_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_BIN,
        charset: "utf8mb4",
        collation: "utf8mb4_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// cp1252 West European
    const LATIN1_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN1_BIN,
        charset: "latin1",
        collation: "latin1_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// cp1252 West European
    const LATIN1_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN1_GENERAL_CI,
        charset: "latin1",
        collation: "latin1_general_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// cp1252 West European
    const LATIN1_GENERAL_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN1_GENERAL_CS,
        charset: "latin1",
        collation: "latin1_general_cs",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Cyrillic
    const CP1251_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1251_BIN,
        charset: "cp1251",
        collation: "cp1251_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Cyrillic
    const CP1251_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1251_GENERAL_CI,
        charset: "cp1251",
        collation: "cp1251_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Cyrillic
    const CP1251_GENERAL_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1251_GENERAL_CS,
        charset: "cp1251",
        collation: "cp1251_general_cs",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Mac West European
    const MACROMAN_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::MACROMAN_BIN,
        charset: "macroman",
        collation: "macroman_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// UTF-16 Unicode
    const UTF16_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_GENERAL_CI,
        charset: "utf16",
        collation: "utf16_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_BIN,
        charset: "utf16",
        collation: "utf16_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// UTF-16LE Unicode
    const UTF16LE_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16LE_GENERAL_CI,
        charset: "utf16le",
        collation: "utf16le_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// Windows Arabic
    const CP1256_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1256_GENERAL_CI,
        charset: "cp1256",
        collation: "cp1256_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Baltic
    const CP1257_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1257_BIN,
        charset: "cp1257",
        collation: "cp1257_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Baltic
    const CP1257_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1257_GENERAL_CI,
        charset: "cp1257",
        collation: "cp1257_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// UTF-32 Unicode
    const UTF32_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_GENERAL_CI,
        charset: "utf32",
        collation: "utf32_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_BIN,
        charset: "utf32",
        collation: "utf32_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// UTF-16LE Unicode
    const UTF16LE_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16LE_BIN,
        charset: "utf16le",
        collation: "utf16le_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// Binary pseudo charset
    const BINARY_COLLATION: Collation<'static> = Collation {
        id: CollationId::BINARY,
        charset: "binary",
        collation: "binary",
        is_default: true,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ARMSCII-8 Armenian
    const ARMSCII8_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::ARMSCII8_BIN,
        charset: "armscii8",
        collation: "armscii8_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// US ASCII
    const ASCII_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::ASCII_BIN,
        charset: "ascii",
        collation: "ascii_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Central European
    const CP1250_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1250_BIN,
        charset: "cp1250",
        collation: "cp1250_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// Windows Arabic
    const CP1256_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1256_BIN,
        charset: "cp1256",
        collation: "cp1256_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// DOS Russian
    const CP866_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP866_BIN,
        charset: "cp866",
        collation: "cp866_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// DEC West European
    const DEC8_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::DEC8_BIN,
        charset: "dec8",
        collation: "dec8_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-7 Greek
    const GREEK_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::GREEK_BIN,
        charset: "greek",
        collation: "greek_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-8 Hebrew
    const HEBREW_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::HEBREW_BIN,
        charset: "hebrew",
        collation: "hebrew_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// HP West European
    const HP8_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::HP8_BIN,
        charset: "hp8",
        collation: "hp8_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// DOS Kamenicky Czech-Slovak
    const KEYBCS2_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::KEYBCS2_BIN,
        charset: "keybcs2",
        collation: "keybcs2_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// KOI8-R Relcom Russian
    const KOI8R_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::KOI8R_BIN,
        charset: "koi8r",
        collation: "koi8r_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// KOI8-U Ukrainian
    const KOI8U_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::KOI8U_BIN,
        charset: "koi8u",
        collation: "koi8u_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// UTF-8 Unicode
    const UTF8MB3_TOLOWER_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_TOLOWER_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_tolower_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 3,
    };
    /// ISO 8859-2 Central European
    const LATIN2_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN2_BIN,
        charset: "latin2",
        collation: "latin2_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-9 Turkish
    const LATIN5_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN5_BIN,
        charset: "latin5",
        collation: "latin5_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// ISO 8859-13 Baltic
    const LATIN7_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN7_BIN,
        charset: "latin7",
        collation: "latin7_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// DOS West European
    const CP850_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP850_BIN,
        charset: "cp850",
        collation: "cp850_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// DOS Central European
    const CP852_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP852_BIN,
        charset: "cp852",
        collation: "cp852_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// 7bit Swedish
    const SWE7_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::SWE7_BIN,
        charset: "swe7",
        collation: "swe7_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// UTF-8 Unicode
    const UTF8MB3_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_BIN,
        charset: "utf8mb3",
        collation: "utf8mb3_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 3,
    };
    /// Big5 Traditional Chinese
    const BIG5_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::BIG5_BIN,
        charset: "big5",
        collation: "big5_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// EUC-KR Korean
    const EUCKR_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::EUCKR_BIN,
        charset: "euckr",
        collation: "euckr_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// GB2312 Simplified Chinese
    const GB2312_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::GB2312_BIN,
        charset: "gb2312",
        collation: "gb2312_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// GBK Simplified Chinese
    const GBK_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::GBK_BIN,
        charset: "gbk",
        collation: "gbk_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// Shift-JIS Japanese
    const SJIS_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::SJIS_BIN,
        charset: "sjis",
        collation: "sjis_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// TIS620 Thai
    const TIS620_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::TIS620_BIN,
        charset: "tis620",
        collation: "tis620_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// UCS-2 Unicode
    const UCS2_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_BIN,
        charset: "ucs2",
        collation: "ucs2_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// EUC-JP Japanese
    const UJIS_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UJIS_BIN,
        charset: "ujis",
        collation: "ujis_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 3,
    };
    /// GEOSTD8 Georgian
    const GEOSTD8_GENERAL_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::GEOSTD8_GENERAL_CI,
        charset: "geostd8",
        collation: "geostd8_general_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// GEOSTD8 Georgian
    const GEOSTD8_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::GEOSTD8_BIN,
        charset: "geostd8",
        collation: "geostd8_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// cp1252 West European
    const LATIN1_SPANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::LATIN1_SPANISH_CI,
        charset: "latin1",
        collation: "latin1_spanish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// SJIS for Windows Japanese
    const CP932_JAPANESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP932_JAPANESE_CI,
        charset: "cp932",
        collation: "cp932_japanese_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// SJIS for Windows Japanese
    const CP932_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP932_BIN,
        charset: "cp932",
        collation: "cp932_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// UJIS for Windows Japanese
    const EUCJPMS_JAPANESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::EUCJPMS_JAPANESE_CI,
        charset: "eucjpms",
        collation: "eucjpms_japanese_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 3,
    };
    /// UJIS for Windows Japanese
    const EUCJPMS_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::EUCJPMS_BIN,
        charset: "eucjpms",
        collation: "eucjpms_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 3,
    };
    /// Windows Central European
    const CP1250_POLISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::CP1250_POLISH_CI,
        charset: "cp1250",
        collation: "cp1250_polish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 1,
    };
    /// UTF-16 Unicode
    const UTF16_UNICODE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_UNICODE_CI,
        charset: "utf16",
        collation: "utf16_unicode_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_ICELANDIC_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_ICELANDIC_CI,
        charset: "utf16",
        collation: "utf16_icelandic_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_LATVIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_LATVIAN_CI,
        charset: "utf16",
        collation: "utf16_latvian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_ROMANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_ROMANIAN_CI,
        charset: "utf16",
        collation: "utf16_romanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_SLOVENIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_SLOVENIAN_CI,
        charset: "utf16",
        collation: "utf16_slovenian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_POLISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_POLISH_CI,
        charset: "utf16",
        collation: "utf16_polish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_ESTONIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_ESTONIAN_CI,
        charset: "utf16",
        collation: "utf16_estonian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_SPANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_SPANISH_CI,
        charset: "utf16",
        collation: "utf16_spanish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_SWEDISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_SWEDISH_CI,
        charset: "utf16",
        collation: "utf16_swedish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_TURKISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_TURKISH_CI,
        charset: "utf16",
        collation: "utf16_turkish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_CZECH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_CZECH_CI,
        charset: "utf16",
        collation: "utf16_czech_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_DANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_DANISH_CI,
        charset: "utf16",
        collation: "utf16_danish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_LITHUANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_LITHUANIAN_CI,
        charset: "utf16",
        collation: "utf16_lithuanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_SLOVAK_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_SLOVAK_CI,
        charset: "utf16",
        collation: "utf16_slovak_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_SPANISH2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_SPANISH2_CI,
        charset: "utf16",
        collation: "utf16_spanish2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_ROMAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_ROMAN_CI,
        charset: "utf16",
        collation: "utf16_roman_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_PERSIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_PERSIAN_CI,
        charset: "utf16",
        collation: "utf16_persian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_ESPERANTO_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_ESPERANTO_CI,
        charset: "utf16",
        collation: "utf16_esperanto_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_HUNGARIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_HUNGARIAN_CI,
        charset: "utf16",
        collation: "utf16_hungarian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_SINHALA_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_SINHALA_CI,
        charset: "utf16",
        collation: "utf16_sinhala_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_GERMAN2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_GERMAN2_CI,
        charset: "utf16",
        collation: "utf16_german2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_CROATIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_CROATIAN_CI,
        charset: "utf16",
        collation: "utf16_croatian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_UNICODE_520_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_UNICODE_520_CI,
        charset: "utf16",
        collation: "utf16_unicode_520_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-16 Unicode
    const UTF16_VIETNAMESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF16_VIETNAMESE_CI,
        charset: "utf16",
        collation: "utf16_vietnamese_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UCS-2 Unicode
    const UCS2_UNICODE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_UNICODE_CI,
        charset: "ucs2",
        collation: "ucs2_unicode_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_ICELANDIC_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_ICELANDIC_CI,
        charset: "ucs2",
        collation: "ucs2_icelandic_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_LATVIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_LATVIAN_CI,
        charset: "ucs2",
        collation: "ucs2_latvian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_ROMANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_ROMANIAN_CI,
        charset: "ucs2",
        collation: "ucs2_romanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_SLOVENIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_SLOVENIAN_CI,
        charset: "ucs2",
        collation: "ucs2_slovenian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_POLISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_POLISH_CI,
        charset: "ucs2",
        collation: "ucs2_polish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_ESTONIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_ESTONIAN_CI,
        charset: "ucs2",
        collation: "ucs2_estonian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_SPANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_SPANISH_CI,
        charset: "ucs2",
        collation: "ucs2_spanish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_SWEDISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_SWEDISH_CI,
        charset: "ucs2",
        collation: "ucs2_swedish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_TURKISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_TURKISH_CI,
        charset: "ucs2",
        collation: "ucs2_turkish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_CZECH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_CZECH_CI,
        charset: "ucs2",
        collation: "ucs2_czech_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_DANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_DANISH_CI,
        charset: "ucs2",
        collation: "ucs2_danish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_LITHUANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_LITHUANIAN_CI,
        charset: "ucs2",
        collation: "ucs2_lithuanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_SLOVAK_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_SLOVAK_CI,
        charset: "ucs2",
        collation: "ucs2_slovak_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_SPANISH2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_SPANISH2_CI,
        charset: "ucs2",
        collation: "ucs2_spanish2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_ROMAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_ROMAN_CI,
        charset: "ucs2",
        collation: "ucs2_roman_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_PERSIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_PERSIAN_CI,
        charset: "ucs2",
        collation: "ucs2_persian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_ESPERANTO_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_ESPERANTO_CI,
        charset: "ucs2",
        collation: "ucs2_esperanto_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_HUNGARIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_HUNGARIAN_CI,
        charset: "ucs2",
        collation: "ucs2_hungarian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_SINHALA_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_SINHALA_CI,
        charset: "ucs2",
        collation: "ucs2_sinhala_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_GERMAN2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_GERMAN2_CI,
        charset: "ucs2",
        collation: "ucs2_german2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_CROATIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_CROATIAN_CI,
        charset: "ucs2",
        collation: "ucs2_croatian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_UNICODE_520_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_UNICODE_520_CI,
        charset: "ucs2",
        collation: "ucs2_unicode_520_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_VIETNAMESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_VIETNAMESE_CI,
        charset: "ucs2",
        collation: "ucs2_vietnamese_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 2,
    };
    /// UCS-2 Unicode
    const UCS2_GENERAL_MYSQL500_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UCS2_GENERAL_MYSQL500_CI,
        charset: "ucs2",
        collation: "ucs2_general_mysql500_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 2,
    };
    /// UTF-32 Unicode
    const UTF32_UNICODE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_UNICODE_CI,
        charset: "utf32",
        collation: "utf32_unicode_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_ICELANDIC_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_ICELANDIC_CI,
        charset: "utf32",
        collation: "utf32_icelandic_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_LATVIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_LATVIAN_CI,
        charset: "utf32",
        collation: "utf32_latvian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_ROMANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_ROMANIAN_CI,
        charset: "utf32",
        collation: "utf32_romanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_SLOVENIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_SLOVENIAN_CI,
        charset: "utf32",
        collation: "utf32_slovenian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_POLISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_POLISH_CI,
        charset: "utf32",
        collation: "utf32_polish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_ESTONIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_ESTONIAN_CI,
        charset: "utf32",
        collation: "utf32_estonian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_SPANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_SPANISH_CI,
        charset: "utf32",
        collation: "utf32_spanish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_SWEDISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_SWEDISH_CI,
        charset: "utf32",
        collation: "utf32_swedish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_TURKISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_TURKISH_CI,
        charset: "utf32",
        collation: "utf32_turkish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_CZECH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_CZECH_CI,
        charset: "utf32",
        collation: "utf32_czech_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_DANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_DANISH_CI,
        charset: "utf32",
        collation: "utf32_danish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_LITHUANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_LITHUANIAN_CI,
        charset: "utf32",
        collation: "utf32_lithuanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_SLOVAK_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_SLOVAK_CI,
        charset: "utf32",
        collation: "utf32_slovak_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_SPANISH2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_SPANISH2_CI,
        charset: "utf32",
        collation: "utf32_spanish2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_ROMAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_ROMAN_CI,
        charset: "utf32",
        collation: "utf32_roman_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_PERSIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_PERSIAN_CI,
        charset: "utf32",
        collation: "utf32_persian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_ESPERANTO_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_ESPERANTO_CI,
        charset: "utf32",
        collation: "utf32_esperanto_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_HUNGARIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_HUNGARIAN_CI,
        charset: "utf32",
        collation: "utf32_hungarian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_SINHALA_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_SINHALA_CI,
        charset: "utf32",
        collation: "utf32_sinhala_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_GERMAN2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_GERMAN2_CI,
        charset: "utf32",
        collation: "utf32_german2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_CROATIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_CROATIAN_CI,
        charset: "utf32",
        collation: "utf32_croatian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_UNICODE_520_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_UNICODE_520_CI,
        charset: "utf32",
        collation: "utf32_unicode_520_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-32 Unicode
    const UTF32_VIETNAMESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF32_VIETNAMESE_CI,
        charset: "utf32",
        collation: "utf32_vietnamese_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB3_UNICODE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_UNICODE_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_unicode_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_ICELANDIC_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_ICELANDIC_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_icelandic_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_LATVIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_LATVIAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_latvian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_ROMANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_ROMANIAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_romanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_SLOVENIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_SLOVENIAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_slovenian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_POLISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_POLISH_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_polish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_ESTONIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_ESTONIAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_estonian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_SPANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_SPANISH_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_spanish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_SWEDISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_SWEDISH_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_swedish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_TURKISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_TURKISH_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_turkish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_CZECH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_CZECH_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_czech_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_DANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_DANISH_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_danish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_LITHUANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_LITHUANIAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_lithuanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_SLOVAK_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_SLOVAK_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_slovak_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_SPANISH2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_SPANISH2_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_spanish2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_ROMAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_ROMAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_roman_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_PERSIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_PERSIAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_persian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_ESPERANTO_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_ESPERANTO_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_esperanto_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_HUNGARIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_HUNGARIAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_hungarian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_SINHALA_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_SINHALA_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_sinhala_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_GERMAN2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_GERMAN2_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_german2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_CROATIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_CROATIAN_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_croatian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_UNICODE_520_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_UNICODE_520_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_unicode_520_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_VIETNAMESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_VIETNAMESE_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_vietnamese_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB3_GENERAL_MYSQL500_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB3_GENERAL_MYSQL500_CI,
        charset: "utf8mb3",
        collation: "utf8mb3_general_mysql500_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 3,
    };
    /// UTF-8 Unicode
    const UTF8MB4_UNICODE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_UNICODE_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_unicode_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ICELANDIC_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ICELANDIC_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_icelandic_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_LATVIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_LATVIAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_latvian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ROMANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ROMANIAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_romanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SLOVENIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SLOVENIAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_slovenian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_POLISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_POLISH_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_polish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ESTONIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ESTONIAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_estonian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SPANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SPANISH_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_spanish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SWEDISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SWEDISH_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_swedish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_TURKISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_TURKISH_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_turkish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_CZECH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_CZECH_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_czech_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_DANISH_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_DANISH_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_danish_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_LITHUANIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_LITHUANIAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_lithuanian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SLOVAK_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SLOVAK_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_slovak_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SPANISH2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SPANISH2_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_spanish2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ROMAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ROMAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_roman_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_PERSIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_PERSIAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_persian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ESPERANTO_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ESPERANTO_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_esperanto_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_HUNGARIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_HUNGARIAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_hungarian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SINHALA_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SINHALA_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_sinhala_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_GERMAN2_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_GERMAN2_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_german2_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_CROATIAN_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_CROATIAN_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_croatian_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_UNICODE_520_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_UNICODE_520_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_unicode_520_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_VIETNAMESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_VIETNAMESE_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_vietnamese_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// China National Standard GB18030
    const GB18030_CHINESE_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::GB18030_CHINESE_CI,
        charset: "gb18030",
        collation: "gb18030_chinese_ci",
        is_default: true,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 2,
        max_len: 4,
    };
    /// China National Standard GB18030
    const GB18030_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::GB18030_BIN,
        charset: "gb18030",
        collation: "gb18030_bin",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// China National Standard GB18030
    const GB18030_UNICODE_520_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::GB18030_UNICODE_520_CI,
        charset: "gb18030",
        collation: "gb18030_unicode_520_ci",
        is_default: false,
        padding: PadAttribute::PadSpace,
        is_compiled: true,
        sort_len: 8,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_0900_ai_ci",
        is_default: true,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_DE_PB_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_DE_PB_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_de_pb_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_IS_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_IS_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_is_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_LV_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_LV_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_lv_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_RO_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_RO_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_ro_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SL_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SL_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_sl_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_PL_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_PL_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_pl_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ET_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ET_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_et_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ES_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ES_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_es_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SV_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SV_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_sv_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_TR_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_TR_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_tr_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_CS_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_CS_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_cs_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_DA_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_DA_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_da_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_LT_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_LT_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_lt_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SK_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SK_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_sk_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ES_TRAD_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ES_TRAD_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_es_trad_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_LA_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_LA_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_la_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_EO_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_EO_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_eo_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_HU_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_HU_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_hu_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_HR_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_HR_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_hr_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_VI_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_VI_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_vi_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_DE_PB_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_DE_PB_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_de_pb_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_IS_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_IS_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_is_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_LV_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_LV_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_lv_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_RO_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_RO_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_ro_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SL_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SL_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_sl_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_PL_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_PL_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_pl_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ET_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ET_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_et_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ES_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ES_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_es_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SV_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SV_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_sv_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_TR_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_TR_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_tr_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_CS_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_CS_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_cs_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_DA_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_DA_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_da_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_LT_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_LT_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_lt_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SK_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SK_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_sk_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ES_TRAD_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ES_TRAD_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_es_trad_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_LA_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_LA_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_la_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_EO_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_EO_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_eo_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_HU_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_HU_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_hu_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_HR_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_HR_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_hr_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_VI_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_VI_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_vi_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_JA_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_JA_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_ja_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_JA_0900_AS_CS_KS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_JA_0900_AS_CS_KS,
        charset: "utf8mb4",
        collation: "utf8mb4_ja_0900_as_cs_ks",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 24,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_0900_AS_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_0900_AS_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_0900_as_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_RU_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_RU_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_ru_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_RU_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_RU_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_ru_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_ZH_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_ZH_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_zh_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_0900_BIN_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_0900_BIN,
        charset: "utf8mb4",
        collation: "utf8mb4_0900_bin",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 1,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_NB_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_NB_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_nb_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_NB_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_NB_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_nb_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_NN_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_NN_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_nn_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_NN_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_NN_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_nn_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SR_LATN_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SR_LATN_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_sr_latn_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_SR_LATN_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_SR_LATN_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_sr_latn_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_BS_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_BS_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_bs_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_BS_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_BS_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_bs_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_BG_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_BG_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_bg_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_BG_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_BG_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_bg_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_GL_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_GL_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_gl_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_GL_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_GL_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_gl_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_MN_CYRL_0900_AI_CI_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_MN_CYRL_0900_AI_CI,
        charset: "utf8mb4",
        collation: "utf8mb4_mn_cyrl_0900_ai_ci",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };
    /// UTF-8 Unicode
    const UTF8MB4_MN_CYRL_0900_AS_CS_COLLATION: Collation<'static> = Collation {
        id: CollationId::UTF8MB4_MN_CYRL_0900_AS_CS,
        charset: "utf8mb4",
        collation: "utf8mb4_mn_cyrl_0900_as_cs",
        is_default: false,
        padding: PadAttribute::PadZero,
        is_compiled: true,
        sort_len: 0,
        max_len: 4,
    };

    /// Get the collation ID.
    pub fn id(&self) -> CollationId {
        self.id
    }

    /// Get the charset.
    pub fn charset(&self) -> &str {
        self.charset
    }

    /// Get the collation.
    pub fn collation(&self) -> &str {
        self.collation
    }

    /// Is the collation default?
    pub fn is_default(&self) -> bool {
        self.is_default
    }

    /// Get the padding attribute.
    pub fn padding(&self) -> PadAttribute {
        self.padding
    }

    /// Is the collation compiled?
    pub fn is_compiled(&self) -> bool {
        self.is_compiled
    }

    /// Get the sort length.
    pub fn sort_len(&self) -> u8 {
        self.sort_len
    }

    /// Get the max length.
    pub fn max_len(&self) -> u8 {
        self.max_len
    }

    /// Resolve collation id into a collation.
    pub const fn resolve(id: CollationId) -> Collation<'static> {
        match id {
            CollationId::UNKNOWN_COLLATION_ID => Self::UNKNOWN_COLLATION,
            CollationId::BIG5_CHINESE_CI => Self::BIG5_CHINESE_CI_COLLATION,
            CollationId::LATIN2_CZECH_CS => Self::LATIN2_CZECH_CS_COLLATION,
            CollationId::DEC8_SWEDISH_CI => Self::DEC8_SWEDISH_CI_COLLATION,
            CollationId::CP850_GENERAL_CI => Self::CP850_GENERAL_CI_COLLATION,
            CollationId::LATIN1_GERMAN1_CI => Self::LATIN1_GERMAN1_CI_COLLATION,
            CollationId::HP8_ENGLISH_CI => Self::HP8_ENGLISH_CI_COLLATION,
            CollationId::KOI8R_GENERAL_CI => Self::KOI8R_GENERAL_CI_COLLATION,
            CollationId::LATIN1_SWEDISH_CI => Self::LATIN1_SWEDISH_CI_COLLATION,
            CollationId::LATIN2_GENERAL_CI => Self::LATIN2_GENERAL_CI_COLLATION,
            CollationId::SWE7_SWEDISH_CI => Self::SWE7_SWEDISH_CI_COLLATION,
            CollationId::ASCII_GENERAL_CI => Self::ASCII_GENERAL_CI_COLLATION,
            CollationId::UJIS_JAPANESE_CI => Self::UJIS_JAPANESE_CI_COLLATION,
            CollationId::SJIS_JAPANESE_CI => Self::SJIS_JAPANESE_CI_COLLATION,
            CollationId::CP1251_BULGARIAN_CI => Self::CP1251_BULGARIAN_CI_COLLATION,
            CollationId::LATIN1_DANISH_CI => Self::LATIN1_DANISH_CI_COLLATION,
            CollationId::HEBREW_GENERAL_CI => Self::HEBREW_GENERAL_CI_COLLATION,
            CollationId::TIS620_THAI_CI => Self::TIS620_THAI_CI_COLLATION,
            CollationId::EUCKR_KOREAN_CI => Self::EUCKR_KOREAN_CI_COLLATION,
            CollationId::LATIN7_ESTONIAN_CS => Self::LATIN7_ESTONIAN_CS_COLLATION,
            CollationId::LATIN2_HUNGARIAN_CI => Self::LATIN2_HUNGARIAN_CI_COLLATION,
            CollationId::KOI8U_GENERAL_CI => Self::KOI8U_GENERAL_CI_COLLATION,
            CollationId::CP1251_UKRAINIAN_CI => Self::CP1251_UKRAINIAN_CI_COLLATION,
            CollationId::GB2312_CHINESE_CI => Self::GB2312_CHINESE_CI_COLLATION,
            CollationId::GREEK_GENERAL_CI => Self::GREEK_GENERAL_CI_COLLATION,
            CollationId::CP1250_GENERAL_CI => Self::CP1250_GENERAL_CI_COLLATION,
            CollationId::LATIN2_CROATIAN_CI => Self::LATIN2_CROATIAN_CI_COLLATION,
            CollationId::GBK_CHINESE_CI => Self::GBK_CHINESE_CI_COLLATION,
            CollationId::CP1257_LITHUANIAN_CI => Self::CP1257_LITHUANIAN_CI_COLLATION,
            CollationId::LATIN5_TURKISH_CI => Self::LATIN5_TURKISH_CI_COLLATION,
            CollationId::LATIN1_GERMAN2_CI => Self::LATIN1_GERMAN2_CI_COLLATION,
            CollationId::ARMSCII8_GENERAL_CI => Self::ARMSCII8_GENERAL_CI_COLLATION,
            CollationId::UTF8MB3_GENERAL_CI => Self::UTF8MB3_GENERAL_CI_COLLATION,
            CollationId::CP1250_CZECH_CS => Self::CP1250_CZECH_CS_COLLATION,
            CollationId::UCS2_GENERAL_CI => Self::UCS2_GENERAL_CI_COLLATION,
            CollationId::CP866_GENERAL_CI => Self::CP866_GENERAL_CI_COLLATION,
            CollationId::KEYBCS2_GENERAL_CI => Self::KEYBCS2_GENERAL_CI_COLLATION,
            CollationId::MACCE_GENERAL_CI => Self::MACCE_GENERAL_CI_COLLATION,
            CollationId::MACROMAN_GENERAL_CI => Self::MACROMAN_GENERAL_CI_COLLATION,
            CollationId::CP852_GENERAL_CI => Self::CP852_GENERAL_CI_COLLATION,
            CollationId::LATIN7_GENERAL_CI => Self::LATIN7_GENERAL_CI_COLLATION,
            CollationId::LATIN7_GENERAL_CS => Self::LATIN7_GENERAL_CS_COLLATION,
            CollationId::MACCE_BIN => Self::MACCE_BIN_COLLATION,
            CollationId::CP1250_CROATIAN_CI => Self::CP1250_CROATIAN_CI_COLLATION,
            CollationId::UTF8MB4_GENERAL_CI => Self::UTF8MB4_GENERAL_CI_COLLATION,
            CollationId::UTF8MB4_BIN => Self::UTF8MB4_BIN_COLLATION,
            CollationId::LATIN1_BIN => Self::LATIN1_BIN_COLLATION,
            CollationId::LATIN1_GENERAL_CI => Self::LATIN1_GENERAL_CI_COLLATION,
            CollationId::LATIN1_GENERAL_CS => Self::LATIN1_GENERAL_CS_COLLATION,
            CollationId::CP1251_BIN => Self::CP1251_BIN_COLLATION,
            CollationId::CP1251_GENERAL_CI => Self::CP1251_GENERAL_CI_COLLATION,
            CollationId::CP1251_GENERAL_CS => Self::CP1251_GENERAL_CS_COLLATION,
            CollationId::MACROMAN_BIN => Self::MACROMAN_BIN_COLLATION,
            CollationId::UTF16_GENERAL_CI => Self::UTF16_GENERAL_CI_COLLATION,
            CollationId::UTF16_BIN => Self::UTF16_BIN_COLLATION,
            CollationId::UTF16LE_GENERAL_CI => Self::UTF16LE_GENERAL_CI_COLLATION,
            CollationId::CP1256_GENERAL_CI => Self::CP1256_GENERAL_CI_COLLATION,
            CollationId::CP1257_BIN => Self::CP1257_BIN_COLLATION,
            CollationId::CP1257_GENERAL_CI => Self::CP1257_GENERAL_CI_COLLATION,
            CollationId::UTF32_GENERAL_CI => Self::UTF32_GENERAL_CI_COLLATION,
            CollationId::UTF32_BIN => Self::UTF32_BIN_COLLATION,
            CollationId::UTF16LE_BIN => Self::UTF16LE_BIN_COLLATION,
            CollationId::BINARY => Self::BINARY_COLLATION,
            CollationId::ARMSCII8_BIN => Self::ARMSCII8_BIN_COLLATION,
            CollationId::ASCII_BIN => Self::ASCII_BIN_COLLATION,
            CollationId::CP1250_BIN => Self::CP1250_BIN_COLLATION,
            CollationId::CP1256_BIN => Self::CP1256_BIN_COLLATION,
            CollationId::CP866_BIN => Self::CP866_BIN_COLLATION,
            CollationId::DEC8_BIN => Self::DEC8_BIN_COLLATION,
            CollationId::GREEK_BIN => Self::GREEK_BIN_COLLATION,
            CollationId::HEBREW_BIN => Self::HEBREW_BIN_COLLATION,
            CollationId::HP8_BIN => Self::HP8_BIN_COLLATION,
            CollationId::KEYBCS2_BIN => Self::KEYBCS2_BIN_COLLATION,
            CollationId::KOI8R_BIN => Self::KOI8R_BIN_COLLATION,
            CollationId::KOI8U_BIN => Self::KOI8U_BIN_COLLATION,
            CollationId::UTF8MB3_TOLOWER_CI => Self::UTF8MB3_TOLOWER_CI_COLLATION,
            CollationId::LATIN2_BIN => Self::LATIN2_BIN_COLLATION,
            CollationId::LATIN5_BIN => Self::LATIN5_BIN_COLLATION,
            CollationId::LATIN7_BIN => Self::LATIN7_BIN_COLLATION,
            CollationId::CP850_BIN => Self::CP850_BIN_COLLATION,
            CollationId::CP852_BIN => Self::CP852_BIN_COLLATION,
            CollationId::SWE7_BIN => Self::SWE7_BIN_COLLATION,
            CollationId::UTF8MB3_BIN => Self::UTF8MB3_BIN_COLLATION,
            CollationId::BIG5_BIN => Self::BIG5_BIN_COLLATION,
            CollationId::EUCKR_BIN => Self::EUCKR_BIN_COLLATION,
            CollationId::GB2312_BIN => Self::GB2312_BIN_COLLATION,
            CollationId::GBK_BIN => Self::GBK_BIN_COLLATION,
            CollationId::SJIS_BIN => Self::SJIS_BIN_COLLATION,
            CollationId::TIS620_BIN => Self::TIS620_BIN_COLLATION,
            CollationId::UCS2_BIN => Self::UCS2_BIN_COLLATION,
            CollationId::UJIS_BIN => Self::UJIS_BIN_COLLATION,
            CollationId::GEOSTD8_GENERAL_CI => Self::GEOSTD8_GENERAL_CI_COLLATION,
            CollationId::GEOSTD8_BIN => Self::GEOSTD8_BIN_COLLATION,
            CollationId::LATIN1_SPANISH_CI => Self::LATIN1_SPANISH_CI_COLLATION,
            CollationId::CP932_JAPANESE_CI => Self::CP932_JAPANESE_CI_COLLATION,
            CollationId::CP932_BIN => Self::CP932_BIN_COLLATION,
            CollationId::EUCJPMS_JAPANESE_CI => Self::EUCJPMS_JAPANESE_CI_COLLATION,
            CollationId::EUCJPMS_BIN => Self::EUCJPMS_BIN_COLLATION,
            CollationId::CP1250_POLISH_CI => Self::CP1250_POLISH_CI_COLLATION,
            CollationId::UTF16_UNICODE_CI => Self::UTF16_UNICODE_CI_COLLATION,
            CollationId::UTF16_ICELANDIC_CI => Self::UTF16_ICELANDIC_CI_COLLATION,
            CollationId::UTF16_LATVIAN_CI => Self::UTF16_LATVIAN_CI_COLLATION,
            CollationId::UTF16_ROMANIAN_CI => Self::UTF16_ROMANIAN_CI_COLLATION,
            CollationId::UTF16_SLOVENIAN_CI => Self::UTF16_SLOVENIAN_CI_COLLATION,
            CollationId::UTF16_POLISH_CI => Self::UTF16_POLISH_CI_COLLATION,
            CollationId::UTF16_ESTONIAN_CI => Self::UTF16_ESTONIAN_CI_COLLATION,
            CollationId::UTF16_SPANISH_CI => Self::UTF16_SPANISH_CI_COLLATION,
            CollationId::UTF16_SWEDISH_CI => Self::UTF16_SWEDISH_CI_COLLATION,
            CollationId::UTF16_TURKISH_CI => Self::UTF16_TURKISH_CI_COLLATION,
            CollationId::UTF16_CZECH_CI => Self::UTF16_CZECH_CI_COLLATION,
            CollationId::UTF16_DANISH_CI => Self::UTF16_DANISH_CI_COLLATION,
            CollationId::UTF16_LITHUANIAN_CI => Self::UTF16_LITHUANIAN_CI_COLLATION,
            CollationId::UTF16_SLOVAK_CI => Self::UTF16_SLOVAK_CI_COLLATION,
            CollationId::UTF16_SPANISH2_CI => Self::UTF16_SPANISH2_CI_COLLATION,
            CollationId::UTF16_ROMAN_CI => Self::UTF16_ROMAN_CI_COLLATION,
            CollationId::UTF16_PERSIAN_CI => Self::UTF16_PERSIAN_CI_COLLATION,
            CollationId::UTF16_ESPERANTO_CI => Self::UTF16_ESPERANTO_CI_COLLATION,
            CollationId::UTF16_HUNGARIAN_CI => Self::UTF16_HUNGARIAN_CI_COLLATION,
            CollationId::UTF16_SINHALA_CI => Self::UTF16_SINHALA_CI_COLLATION,
            CollationId::UTF16_GERMAN2_CI => Self::UTF16_GERMAN2_CI_COLLATION,
            CollationId::UTF16_CROATIAN_CI => Self::UTF16_CROATIAN_CI_COLLATION,
            CollationId::UTF16_UNICODE_520_CI => Self::UTF16_UNICODE_520_CI_COLLATION,
            CollationId::UTF16_VIETNAMESE_CI => Self::UTF16_VIETNAMESE_CI_COLLATION,
            CollationId::UCS2_UNICODE_CI => Self::UCS2_UNICODE_CI_COLLATION,
            CollationId::UCS2_ICELANDIC_CI => Self::UCS2_ICELANDIC_CI_COLLATION,
            CollationId::UCS2_LATVIAN_CI => Self::UCS2_LATVIAN_CI_COLLATION,
            CollationId::UCS2_ROMANIAN_CI => Self::UCS2_ROMANIAN_CI_COLLATION,
            CollationId::UCS2_SLOVENIAN_CI => Self::UCS2_SLOVENIAN_CI_COLLATION,
            CollationId::UCS2_POLISH_CI => Self::UCS2_POLISH_CI_COLLATION,
            CollationId::UCS2_ESTONIAN_CI => Self::UCS2_ESTONIAN_CI_COLLATION,
            CollationId::UCS2_SPANISH_CI => Self::UCS2_SPANISH_CI_COLLATION,
            CollationId::UCS2_SWEDISH_CI => Self::UCS2_SWEDISH_CI_COLLATION,
            CollationId::UCS2_TURKISH_CI => Self::UCS2_TURKISH_CI_COLLATION,
            CollationId::UCS2_CZECH_CI => Self::UCS2_CZECH_CI_COLLATION,
            CollationId::UCS2_DANISH_CI => Self::UCS2_DANISH_CI_COLLATION,
            CollationId::UCS2_LITHUANIAN_CI => Self::UCS2_LITHUANIAN_CI_COLLATION,
            CollationId::UCS2_SLOVAK_CI => Self::UCS2_SLOVAK_CI_COLLATION,
            CollationId::UCS2_SPANISH2_CI => Self::UCS2_SPANISH2_CI_COLLATION,
            CollationId::UCS2_ROMAN_CI => Self::UCS2_ROMAN_CI_COLLATION,
            CollationId::UCS2_PERSIAN_CI => Self::UCS2_PERSIAN_CI_COLLATION,
            CollationId::UCS2_ESPERANTO_CI => Self::UCS2_ESPERANTO_CI_COLLATION,
            CollationId::UCS2_HUNGARIAN_CI => Self::UCS2_HUNGARIAN_CI_COLLATION,
            CollationId::UCS2_SINHALA_CI => Self::UCS2_SINHALA_CI_COLLATION,
            CollationId::UCS2_GERMAN2_CI => Self::UCS2_GERMAN2_CI_COLLATION,
            CollationId::UCS2_CROATIAN_CI => Self::UCS2_CROATIAN_CI_COLLATION,
            CollationId::UCS2_UNICODE_520_CI => Self::UCS2_UNICODE_520_CI_COLLATION,
            CollationId::UCS2_VIETNAMESE_CI => Self::UCS2_VIETNAMESE_CI_COLLATION,
            CollationId::UCS2_GENERAL_MYSQL500_CI => Self::UCS2_GENERAL_MYSQL500_CI_COLLATION,
            CollationId::UTF32_UNICODE_CI => Self::UTF32_UNICODE_CI_COLLATION,
            CollationId::UTF32_ICELANDIC_CI => Self::UTF32_ICELANDIC_CI_COLLATION,
            CollationId::UTF32_LATVIAN_CI => Self::UTF32_LATVIAN_CI_COLLATION,
            CollationId::UTF32_ROMANIAN_CI => Self::UTF32_ROMANIAN_CI_COLLATION,
            CollationId::UTF32_SLOVENIAN_CI => Self::UTF32_SLOVENIAN_CI_COLLATION,
            CollationId::UTF32_POLISH_CI => Self::UTF32_POLISH_CI_COLLATION,
            CollationId::UTF32_ESTONIAN_CI => Self::UTF32_ESTONIAN_CI_COLLATION,
            CollationId::UTF32_SPANISH_CI => Self::UTF32_SPANISH_CI_COLLATION,
            CollationId::UTF32_SWEDISH_CI => Self::UTF32_SWEDISH_CI_COLLATION,
            CollationId::UTF32_TURKISH_CI => Self::UTF32_TURKISH_CI_COLLATION,
            CollationId::UTF32_CZECH_CI => Self::UTF32_CZECH_CI_COLLATION,
            CollationId::UTF32_DANISH_CI => Self::UTF32_DANISH_CI_COLLATION,
            CollationId::UTF32_LITHUANIAN_CI => Self::UTF32_LITHUANIAN_CI_COLLATION,
            CollationId::UTF32_SLOVAK_CI => Self::UTF32_SLOVAK_CI_COLLATION,
            CollationId::UTF32_SPANISH2_CI => Self::UTF32_SPANISH2_CI_COLLATION,
            CollationId::UTF32_ROMAN_CI => Self::UTF32_ROMAN_CI_COLLATION,
            CollationId::UTF32_PERSIAN_CI => Self::UTF32_PERSIAN_CI_COLLATION,
            CollationId::UTF32_ESPERANTO_CI => Self::UTF32_ESPERANTO_CI_COLLATION,
            CollationId::UTF32_HUNGARIAN_CI => Self::UTF32_HUNGARIAN_CI_COLLATION,
            CollationId::UTF32_SINHALA_CI => Self::UTF32_SINHALA_CI_COLLATION,
            CollationId::UTF32_GERMAN2_CI => Self::UTF32_GERMAN2_CI_COLLATION,
            CollationId::UTF32_CROATIAN_CI => Self::UTF32_CROATIAN_CI_COLLATION,
            CollationId::UTF32_UNICODE_520_CI => Self::UTF32_UNICODE_520_CI_COLLATION,
            CollationId::UTF32_VIETNAMESE_CI => Self::UTF32_VIETNAMESE_CI_COLLATION,
            CollationId::UTF8MB3_UNICODE_CI => Self::UTF8MB3_UNICODE_CI_COLLATION,
            CollationId::UTF8MB3_ICELANDIC_CI => Self::UTF8MB3_ICELANDIC_CI_COLLATION,
            CollationId::UTF8MB3_LATVIAN_CI => Self::UTF8MB3_LATVIAN_CI_COLLATION,
            CollationId::UTF8MB3_ROMANIAN_CI => Self::UTF8MB3_ROMANIAN_CI_COLLATION,
            CollationId::UTF8MB3_SLOVENIAN_CI => Self::UTF8MB3_SLOVENIAN_CI_COLLATION,
            CollationId::UTF8MB3_POLISH_CI => Self::UTF8MB3_POLISH_CI_COLLATION,
            CollationId::UTF8MB3_ESTONIAN_CI => Self::UTF8MB3_ESTONIAN_CI_COLLATION,
            CollationId::UTF8MB3_SPANISH_CI => Self::UTF8MB3_SPANISH_CI_COLLATION,
            CollationId::UTF8MB3_SWEDISH_CI => Self::UTF8MB3_SWEDISH_CI_COLLATION,
            CollationId::UTF8MB3_TURKISH_CI => Self::UTF8MB3_TURKISH_CI_COLLATION,
            CollationId::UTF8MB3_CZECH_CI => Self::UTF8MB3_CZECH_CI_COLLATION,
            CollationId::UTF8MB3_DANISH_CI => Self::UTF8MB3_DANISH_CI_COLLATION,
            CollationId::UTF8MB3_LITHUANIAN_CI => Self::UTF8MB3_LITHUANIAN_CI_COLLATION,
            CollationId::UTF8MB3_SLOVAK_CI => Self::UTF8MB3_SLOVAK_CI_COLLATION,
            CollationId::UTF8MB3_SPANISH2_CI => Self::UTF8MB3_SPANISH2_CI_COLLATION,
            CollationId::UTF8MB3_ROMAN_CI => Self::UTF8MB3_ROMAN_CI_COLLATION,
            CollationId::UTF8MB3_PERSIAN_CI => Self::UTF8MB3_PERSIAN_CI_COLLATION,
            CollationId::UTF8MB3_ESPERANTO_CI => Self::UTF8MB3_ESPERANTO_CI_COLLATION,
            CollationId::UTF8MB3_HUNGARIAN_CI => Self::UTF8MB3_HUNGARIAN_CI_COLLATION,
            CollationId::UTF8MB3_SINHALA_CI => Self::UTF8MB3_SINHALA_CI_COLLATION,
            CollationId::UTF8MB3_GERMAN2_CI => Self::UTF8MB3_GERMAN2_CI_COLLATION,
            CollationId::UTF8MB3_CROATIAN_CI => Self::UTF8MB3_CROATIAN_CI_COLLATION,
            CollationId::UTF8MB3_UNICODE_520_CI => Self::UTF8MB3_UNICODE_520_CI_COLLATION,
            CollationId::UTF8MB3_VIETNAMESE_CI => Self::UTF8MB3_VIETNAMESE_CI_COLLATION,
            CollationId::UTF8MB3_GENERAL_MYSQL500_CI => Self::UTF8MB3_GENERAL_MYSQL500_CI_COLLATION,
            CollationId::UTF8MB4_UNICODE_CI => Self::UTF8MB4_UNICODE_CI_COLLATION,
            CollationId::UTF8MB4_ICELANDIC_CI => Self::UTF8MB4_ICELANDIC_CI_COLLATION,
            CollationId::UTF8MB4_LATVIAN_CI => Self::UTF8MB4_LATVIAN_CI_COLLATION,
            CollationId::UTF8MB4_ROMANIAN_CI => Self::UTF8MB4_ROMANIAN_CI_COLLATION,
            CollationId::UTF8MB4_SLOVENIAN_CI => Self::UTF8MB4_SLOVENIAN_CI_COLLATION,
            CollationId::UTF8MB4_POLISH_CI => Self::UTF8MB4_POLISH_CI_COLLATION,
            CollationId::UTF8MB4_ESTONIAN_CI => Self::UTF8MB4_ESTONIAN_CI_COLLATION,
            CollationId::UTF8MB4_SPANISH_CI => Self::UTF8MB4_SPANISH_CI_COLLATION,
            CollationId::UTF8MB4_SWEDISH_CI => Self::UTF8MB4_SWEDISH_CI_COLLATION,
            CollationId::UTF8MB4_TURKISH_CI => Self::UTF8MB4_TURKISH_CI_COLLATION,
            CollationId::UTF8MB4_CZECH_CI => Self::UTF8MB4_CZECH_CI_COLLATION,
            CollationId::UTF8MB4_DANISH_CI => Self::UTF8MB4_DANISH_CI_COLLATION,
            CollationId::UTF8MB4_LITHUANIAN_CI => Self::UTF8MB4_LITHUANIAN_CI_COLLATION,
            CollationId::UTF8MB4_SLOVAK_CI => Self::UTF8MB4_SLOVAK_CI_COLLATION,
            CollationId::UTF8MB4_SPANISH2_CI => Self::UTF8MB4_SPANISH2_CI_COLLATION,
            CollationId::UTF8MB4_ROMAN_CI => Self::UTF8MB4_ROMAN_CI_COLLATION,
            CollationId::UTF8MB4_PERSIAN_CI => Self::UTF8MB4_PERSIAN_CI_COLLATION,
            CollationId::UTF8MB4_ESPERANTO_CI => Self::UTF8MB4_ESPERANTO_CI_COLLATION,
            CollationId::UTF8MB4_HUNGARIAN_CI => Self::UTF8MB4_HUNGARIAN_CI_COLLATION,
            CollationId::UTF8MB4_SINHALA_CI => Self::UTF8MB4_SINHALA_CI_COLLATION,
            CollationId::UTF8MB4_GERMAN2_CI => Self::UTF8MB4_GERMAN2_CI_COLLATION,
            CollationId::UTF8MB4_CROATIAN_CI => Self::UTF8MB4_CROATIAN_CI_COLLATION,
            CollationId::UTF8MB4_UNICODE_520_CI => Self::UTF8MB4_UNICODE_520_CI_COLLATION,
            CollationId::UTF8MB4_VIETNAMESE_CI => Self::UTF8MB4_VIETNAMESE_CI_COLLATION,
            CollationId::GB18030_CHINESE_CI => Self::GB18030_CHINESE_CI_COLLATION,
            CollationId::GB18030_BIN => Self::GB18030_BIN_COLLATION,
            CollationId::GB18030_UNICODE_520_CI => Self::GB18030_UNICODE_520_CI_COLLATION,
            CollationId::UTF8MB4_0900_AI_CI => Self::UTF8MB4_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_DE_PB_0900_AI_CI => Self::UTF8MB4_DE_PB_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_IS_0900_AI_CI => Self::UTF8MB4_IS_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_LV_0900_AI_CI => Self::UTF8MB4_LV_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_RO_0900_AI_CI => Self::UTF8MB4_RO_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_SL_0900_AI_CI => Self::UTF8MB4_SL_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_PL_0900_AI_CI => Self::UTF8MB4_PL_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_ET_0900_AI_CI => Self::UTF8MB4_ET_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_ES_0900_AI_CI => Self::UTF8MB4_ES_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_SV_0900_AI_CI => Self::UTF8MB4_SV_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_TR_0900_AI_CI => Self::UTF8MB4_TR_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_CS_0900_AI_CI => Self::UTF8MB4_CS_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_DA_0900_AI_CI => Self::UTF8MB4_DA_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_LT_0900_AI_CI => Self::UTF8MB4_LT_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_SK_0900_AI_CI => Self::UTF8MB4_SK_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_ES_TRAD_0900_AI_CI => Self::UTF8MB4_ES_TRAD_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_LA_0900_AI_CI => Self::UTF8MB4_LA_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_EO_0900_AI_CI => Self::UTF8MB4_EO_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_HU_0900_AI_CI => Self::UTF8MB4_HU_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_HR_0900_AI_CI => Self::UTF8MB4_HR_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_VI_0900_AI_CI => Self::UTF8MB4_VI_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_0900_AS_CS => Self::UTF8MB4_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_DE_PB_0900_AS_CS => Self::UTF8MB4_DE_PB_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_IS_0900_AS_CS => Self::UTF8MB4_IS_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_LV_0900_AS_CS => Self::UTF8MB4_LV_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_RO_0900_AS_CS => Self::UTF8MB4_RO_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_SL_0900_AS_CS => Self::UTF8MB4_SL_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_PL_0900_AS_CS => Self::UTF8MB4_PL_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_ET_0900_AS_CS => Self::UTF8MB4_ET_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_ES_0900_AS_CS => Self::UTF8MB4_ES_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_SV_0900_AS_CS => Self::UTF8MB4_SV_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_TR_0900_AS_CS => Self::UTF8MB4_TR_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_CS_0900_AS_CS => Self::UTF8MB4_CS_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_DA_0900_AS_CS => Self::UTF8MB4_DA_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_LT_0900_AS_CS => Self::UTF8MB4_LT_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_SK_0900_AS_CS => Self::UTF8MB4_SK_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_ES_TRAD_0900_AS_CS => Self::UTF8MB4_ES_TRAD_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_LA_0900_AS_CS => Self::UTF8MB4_LA_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_EO_0900_AS_CS => Self::UTF8MB4_EO_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_HU_0900_AS_CS => Self::UTF8MB4_HU_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_HR_0900_AS_CS => Self::UTF8MB4_HR_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_VI_0900_AS_CS => Self::UTF8MB4_VI_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_JA_0900_AS_CS => Self::UTF8MB4_JA_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_JA_0900_AS_CS_KS => Self::UTF8MB4_JA_0900_AS_CS_KS_COLLATION,
            CollationId::UTF8MB4_0900_AS_CI => Self::UTF8MB4_0900_AS_CI_COLLATION,
            CollationId::UTF8MB4_RU_0900_AI_CI => Self::UTF8MB4_RU_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_RU_0900_AS_CS => Self::UTF8MB4_RU_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_ZH_0900_AS_CS => Self::UTF8MB4_ZH_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_0900_BIN => Self::UTF8MB4_0900_BIN_COLLATION,
            CollationId::UTF8MB4_NB_0900_AI_CI => Self::UTF8MB4_NB_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_NB_0900_AS_CS => Self::UTF8MB4_NB_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_NN_0900_AI_CI => Self::UTF8MB4_NN_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_NN_0900_AS_CS => Self::UTF8MB4_NN_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_SR_LATN_0900_AI_CI => Self::UTF8MB4_SR_LATN_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_SR_LATN_0900_AS_CS => Self::UTF8MB4_SR_LATN_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_BS_0900_AI_CI => Self::UTF8MB4_BS_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_BS_0900_AS_CS => Self::UTF8MB4_BS_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_BG_0900_AI_CI => Self::UTF8MB4_BG_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_BG_0900_AS_CS => Self::UTF8MB4_BG_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_GL_0900_AI_CI => Self::UTF8MB4_GL_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_GL_0900_AS_CS => Self::UTF8MB4_GL_0900_AS_CS_COLLATION,
            CollationId::UTF8MB4_MN_CYRL_0900_AI_CI => Self::UTF8MB4_MN_CYRL_0900_AI_CI_COLLATION,
            CollationId::UTF8MB4_MN_CYRL_0900_AS_CS => Self::UTF8MB4_MN_CYRL_0900_AS_CS_COLLATION,
        }
    }
}

impl From<CollationId> for Collation<'static> {
    /// Convert a collation ID to a collation.
    fn from(value: CollationId) -> Self {
        Collation::resolve(value)
    }
}
