#!/usr/bin/perl -w 

use strict;

use Text::CSV qw( csv );

my $url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/';

my $csv_prefix = 'time_series_covid19_';
my $csv_suffix = '_global.csv';

my %sheets = (
    'confirmed' => 'confirmed',
    'recovered' => 'recovered',
    'deaths'    => 'death'
    );

my $place_id_map = {
    'China' => {
	'Hubei'          => 1,
	'Guangdong'      => 2,
	'Henan'          => 3,
	'Hunan'          => 4,
	'Jiangxi'        => 5,
	'Anhui'          => 6,
	'Chongqing'      => 7,
	'Jiangsu'        => 8,
	'Shandong'       => 9,
	'Sichuan'        => 10,
	'Beijing'        => 11,
	'Shanghai'       => 12,
	'Fujian'         => 13,
	'Heilongjiang'   => 14,
	'Shaanxi'        => 15,
	'Guangxi'        => 16,
	'Hebei'          => 17,
	'Yunnan'         => 18,
	'Hainan'         => 19,
	'Liaoning'       => 20,
	'Shanxi'         => 21,
	'Tianjin'        => 22,
	'Guizhou'        => 23,
	'Gansu'          => 24,
	'Jilin'          => 25,
	'Inner Mongolia' => 26,
	'Ningxia'        => 27,
	'Xinjiang'       => 28,
	'Qinghai'        => 34,
	'Tibet'          => 59,
	'Zhejiang'       => 68,
	'Hong Kong'      => 32,
        'Macau'          => 38,    
    },
    'Singapore' => {
	'' => 29,
    },
    'Thailand' => {
	'' => 30,
    },
    'Japan' => {
	'' => 31,
    },
    'Korea, South' => {
	'' => 33,
    },
    'Germany' => {
	'' => 35,
    },
    'Malaysia' => {
	'' => 36,
    },
    'Taiwan*' => {
	'' => 37,
    },
    'Vietnam' => {
	'' => 39,
    },
    'France' => {
	'' => 40,
        'St Martin' => 276,
	'Saint Barthelemy' => 158,
        'French Polynesia' => 458,
	'French Guiana' => 471,
	'Guadeloupe' => 502,
	'Mayotte' => 498,
	'Reunion' => 399,
	'New Caledonia' => 524,
	'Martinique' => 559,
	'Saint Pierre and Miquelon' => 583,
    },
    'Australia' => {
	'New South Wales' => 41,
	'Victoria' => 42,
	'Queensland' => 43,
	'South Australia' => 45,
	'Western Australia' => 117,
	'Tasmania' => 134,    
	'Northern Territory' => 160,
	'Australian Capital Territory' => 456,    
    },
    'India' => {
	'' => 44,
    },
    'Canada' => {
	'' => 570,
	'Ontario' => 46,
	'British Columbia' => 56,
	'Quebec' => 109,
	'Alberta' => 224,
	'New Brunswick' => 395,
	'Manitoba' => 461,
	'Saskatchewan' => 462,
	'Grand Princess' => 468,
	'Newfoundland and Labrador' => 493,
	'Prince Edward Island' => 494,
	'Northwest Territories' => 539,
	'Nova Scotia' => 501,
	'Diamond Princess' => 561,
	'Recovered' => 569,
	'Yukon' => 571,    
    },
    'Italy' => {
	'' => 47,
    },
    'Philippines' => {
	'' => 48,
    },
    'Russia' => {
	'' => 49,
    },
    'United Kingdom' => {
	'' => 50,
	'Channel Islands' => 329,
	'Gibraltar' => 162,
	'Cayman Islands' => 459,
	'Montserrat' => 523,
	'Bermuda' => 525,
	'Isle of Man' => 537,
	# 'Guernsey' => 484,
	# 'Jersey' => 475,
	'Anguilla' => 573,
	'British Virgin Islands' => 574,
	'Turks and Caicos Islands' => 575,
	'Falkland Islands (Malvinas)' => 582,    
    },
    'US' => {
	'' => 553,    
	# 'San Benito, CA' => 52,
	# 'Orange County, CA'  => 53,
	# 'Los Angeles, CA' => 74,
	# 'Santa Clara County, CA' => 51,
	# 'Suffolk County, MA'  => 63,
	# 'King County, WA' => 70,
	# 'Cook County, IL' => 71,
	# 'San Diego County, CA' => 75,
	# 'Humboldt County, CA' => 85,
	# 'Sacramento County, CA' => 86,
	# 'Snohomish County, WA' => 123,
	# 'Providence County, RI' => 129,   
	# 'Grafton County, NH' => 139,
	# 'Hillsborough, FL' => 140,
	# 'New York County, NY' => 141,
	# 'Placer County, CA' => 142,
	# 'San Mateo, CA' => 143,
	# 'Sonoma County, CA' => 145,
	# 'Umatilla, OR' => 146,
	# 'Fulton County, GA' => 147,
	# 'Washington County, OR' => 122,
	# 'Norfolk County, MA' => 152,
	# 'Alameda County, CA' => 153,
	# 'Maricopa County, AZ' => 154,
	# 'Wake County, NC' => 155,
	# 'Westchester County, NY' => 156,
	# 'Contra Costa County, CA' => 166,
	# 'Bergen County, NJ' => 170,
	# 'Harris County, TX' => 171,
	# 'San Francisco County, CA' => 172,
	# 'Clark County, NV' => 174,
	# 'Fort Bend County, TX' => 175,
	# 'Grant County, WA' => 176,
	# 'Santa Rosa County, FL' => 178,
	# 'Williamson County, TN' => 179,
	# 'Montgomery County, MD' => 209,
	# 'Denver County, CO' => 184,
	# 'Summit County, CO' => 185,
	# 'Chatham County, NC' => 194,
	# 'Delaware County, PA' => 195,
	# 'Douglas County, NE' => 196,
	# 'Fayette County, KY' => 197,
	# 'Marion County, IN' => 199,
	# 'Middlesex County, MA' => 200,
	# 'Nassau County, NY' => 201,
	# 'Ramsey County, MN' => 203,
	# 'Washoe County, NV' => 204,
	# 'Wayne County, PA' => 205,
	# 'Yolo County, CA' => 206,
	# 'Grand Princess' => 211,
	# 'Douglas County, CO' => 214,
	# 'Broward County, FL' => 218,
	# 'Lee County, FL' => 220,
	# 'Pinal County, AZ' => 221,
	# 'Rockland County, NY' => 222,
	# 'Saratoga County, NY' => 223,
	# 'Charleston County, SC' => 225,
	# 'Clark County, WA' => 226,
	# 'Cobb County, GA' => 227,
	# 'Davis County, UT' => 228,
	# 'El Paso County, CO' => 229,
	# 'Honolulu County, HI' => 230,
	# 'Jackson County, OR' => 231,
	# 'Jefferson County, WA' => 232,
	# 'Kershaw County, SC' => 233,
	# 'Klamath County, OR' => 234,
	# 'Madera County, CA' => 235,
	# 'Pierce County, WA' => 236,
	# 'Tulsa County, OK' => 239,
	# 'Montgomery County, PA' => 244,
	# 'Fairfax County, VA' => 246,
	# 'Rockingham County, NH' => 247,
	# 'Washington, D.C.' => 248,
	# 'Berkshire County, MA' => 251,
	# 'Davidson County, TN' => 252,
	# 'Douglas County, OR' => 253,
	# 'Fresno County, CA' => 254,
	# 'Harford County, MD' => 255,
	# 'Hendricks County, IN' => 256,
	# 'Hudson County, NJ' => 257,
	# 'Johnson County, KS' => 258,
	# 'Kittitas County, WA' => 259,
	# 'Manatee County, FL' => 260,
        # 'Marion County, OR' => 261,
	# 'Okaloosa County, FL' => 262,
	# 'Polk County, GA' => 198,
	# 'Riverside County, CA' => 264,
	# 'Shelby County, TN' => 265,
	# 'St. Louis County, MO' => 267,
	# 'Suffolk County, NY' => 268,
	# 'Ulster County, NY' => 269,
	# 'Bennington County, VT' => 270,
	# 'Volusia County, FL' => 272,
	# 'Johnson County, IA' => 273,
	# 'Harrison County, KY' => 277,
	# 'Carver County, MN' => 280,
	# 'Charlotte County, FL' => 281,
	# 'Cherokee County, GA' => 282,
	# 'Collin County, TX' => 283,
        # 'Jefferson County, KY' => 284,
	# 'Jefferson Parish, LA' => 285,
	# 'Shasta County, CA' => 286,
	# 'Spartanburg County, SC' => 287,
	# 'Washington' => 290,
	# 'New York' => 291,
	# 'California' => 292,
	# 'Massachusetts' => 293,
	# 'Diamond Princess' => 294,
	# 'Georgia' => 297,
	# 'Colorado' => 298,
	# 'Florida' => 299,
	# 'New Jersey' => 300,
	# 'Oregon' => 301,
	# 'Texas' => 302,
	# 'Illinois' => 303,
	# 'Pennsylvania' => 304,
	# 'Iowa' => 305,
	# 'Maryland' => 306,
	# 'North Carolina' => 307,
	# 'South Carolina' => 308,
	# 'Tennessee' => 309,
	# 'Virginia' => 310,
	# 'Arizona' => 311,
	# 'Indiana' => 312,
	# 'Kentucky' => 313,
	# 'District of Columbia' => 314,
        # 'Nevada' => 315,
	# 'New Hampshire' => 316,
	# 'Minnesota' => 318,
	# 'Nebraska' => 319,
	# 'Ohio' => 320,
	# 'Rhode Island' => 321,
	# 'Wisconsin' => 322,
	# 'Connecticut' => 324,
	# 'Hawaii' => 325,
	# 'Oklahoma' => 326,
	# 'Utah' => 327,
	# 'Kansas' => 333,
	# 'Louisiana' => 334,
	# 'Missouri' => 335,
	# 'Vermont' => 336,
	# 'Alaska' => 337,
	# 'Arkansas' => 338,
	# 'Delaware' => 339,
	# 'Idaho' => 340,
	# 'Maine' => 341,
	# 'Michigan' => 342,
	# 'Mississippi' => 343,
	# 'Montana' => 344,
	# 'New Mexico' => 345,
	# 'North Dakota' => 346,
	# 'South Dakota' => 347,
	# 'West Virginia' => 348,
	# 'Wyoming' => 349,
	# 'Kitsap, WA' => 350,
	# 'Solano, CA' => 351,
	# 'Santa Cruz, CA' => 352,
	# 'Napa, CA' => 353,
	# 'Ventura, CA' => 354,
	# 'Worcester, MA' => 355,
	# 'Gwinnett, GA' => 356,
	# 'DeKalb, GA' => 357,
	# 'Floyd, GA' => 358,
	# 'Fayette, GA' => 359,
	# 'Gregg, TX' => 360,
	# 'Monmouth, NJ' => 361,
	# 'Burlington, NJ' => 362,
	# 'Camden, NJ' => 363,
	# 'Passaic, NJ' => 364,
	# 'Union, NJ' => 365,
	# 'Eagle, CO' => 366,
	# 'Larimer, CO' => 367,
	# 'Arapahoe, CO' => 368,
	# 'Gunnison, CO' => 369,
	# 'Kane, IL' => 370,
	# 'Monroe, PA' => 371,
	# 'Philadelphia, PA' => 372,
	# 'Norfolk, VA' => 373,
	# 'Arlington, VA' => 374,
	# 'Spotsylvania, VA' => 375,
        # 'Loudoun, VA' => 376,
	# 'Pottawattamie, IA' => 378,
	# 'Pima, AZ' => 380,
	# 'Noble, IN' => 381,
	# 'Adams, IN' => 382,
	# 'Boone, IN' => 383,
	# 'Dane, WI' => 384,
	# 'Pierce, WI' => 385,
	# 'Cuyahoga, OH' => 386,
	# 'Weber, UT' => 387,
	# 'Prince George\'s, MD' => 401,
	# 'Camden, NC' => 402,
	# 'Skagit, WA' => 403,
	# 'Thurston, WA' => 404,
	# 'Island, WA' => 405,
	# 'Whatcom, WA' => 406,
	# 'Marin, CA' => 407,
	# 'Calaveras, CA' => 408,
	# 'Stanislaus, CA' => 409,
	# 'San Joaquin, CA' => 410,
	# 'Essex, MA' => 411,
	# 'Charlton, GA' => 412,
	# 'Collier, FL' => 413,
	# 'Pinellas, FL' => 414,
	# 'Alachua, FL' => 415,
	# 'Nassau, FL' => 416,
	# 'Pasco, FL' => 417,
	# 'Dallas, TX' => 418,
	# 'Tarrant, TX' => 419,
	# 'Montgomery, TX' => 420,
	# 'Middlesex, NJ' => 421,
	# 'Jefferson, CO' => 422,
	# 'Multnomah, OR' => 423,
	# 'Polk, OR' => 424,
	# 'Deschutes, OR' => 425,
	# 'McHenry, IL' => 426,
	# 'Lake, IL' => 427,
	# 'Bucks, PA' => 428,
	# 'Hanover, VA' => 429,
	# 'Lancaster, SC' => 430,
	# 'Sullivan, TN' => 431,
	# 'Johnson, IN' => 432,
	# 'Howard, IN' => 433,
	# 'St. Joseph, IN' => 434,
	# 'Knox, NE' => 435,
	# 'Stark, OH' => 436,
	# 'Anoka, MN' => 437,
	# 'Olmsted, MN' => 438,
	# 'Summit, UT' => 439,
	# 'Fairfield, CT' => 440,
	# 'Litchfield, CT' => 441,
	# 'Orleans, LA' => 442,
	# 'Pennington, SD' => 443,
	# 'Beadle, SD' => 444,
	# 'Charles Mix, SD' => 445,
	# 'Davison, SD' => 446,
	# 'Minnehaha, SD' => 447,
	# 'Bon Homme, SD' => 448,
	# 'Socorro, NM' => 449,
	# 'Bernalillo, NM' => 450,
	# 'Oakland, MI' => 451,
	# 'Wayne, MI' => 452,
	# 'New Castle, DE' => 453,
	# 'Alabama' => 470,
	# 'Puerto Rico' => 474,
	# 'Virgin Islands' => 490,
	# 'Guam' => 513,
	# 'United States Virgin Islands' => 552,
    },
    'Belgium' => {
	'' => 54,
    },
    'Cambodia' => {
	'' => 55,
    },
    'Finland' => {
	'' => 58,
    },
    'Nepal' => {
	'' => 181,
    },
    'Spain' => {
	'' => 60,
    },
    'Sri Lanka' => {
	'' => 61,
    },
    'Sweden' => {
	'' => 62,
    },
    'United Arab Emirates' => {
	'' => 64,
    },
    'Egypt' => {
	'' => 77,
    },
    'Iran' => {
        '' => 79,
    },
    'Israel' => {
	'' => 83,
    },
    'Lebanon' => {
	'' => 84,
    },
    'Iraq' => {
	'' => 87,
    },
    'Oman' => {
	'' => 89, 
    },
    'Afghanistan' => {
	'' => 90,
    },
    'Bahrain' => {
	'' => 91,
    },
    'Kuwait' => {
	'' => 92,
    },
    'Austria' => {
	'' => 93,
    },
    'Algeria' => {
	'' => 94,
    },
    'Croatia' => {
	'' => 95,
    },
    'Switzerland' => {
	'' => 96,
    },
    'Pakistan' => {
        '' => 97,
    },
    'Brazil' => {
        '' => 98,
    },
    'Georgia' => {
        '' => 99,
    },
    'Greece' => {
        '' => 100,
    },
    'North Macedonia' => {
        '' => 101,
    },
    'Norway' => {
        '' => 102,
    },
    'Romania' => {
        '' => 103,
    },
    'Estonia' => {
        '' => 105,
    },
    'Netherlands' => {
        '' => 106,
        'Curacao' => 480,
	'Aruba' => 466,
	'Sint Maarten' => 534,
	'Bonaire, Sint Eustatius and Saba' => 580,    
    },
    'San Marino' => {
        '' => 107,
    },  
    'Azerbaijan' => {
        '' => 108,
    },
    'Belarus' => {
	'' => 182,
    },
    'Iceland' => {
        '' => 111,
    },
    'Lithuania' => {
        '' => 112,
    },
    'Mexico' => {
        '' => 113,
    },
    'New Zealand' => {
        '' => 114,
    },
    'Nigeria' => {
        '' => 115,
    },
    'Ireland' => {
	'' => 118,
    },
    'Luxembourg' => {
	'' => 119,
    },
    'Monaco' => {
	'' => 120,
    },
    'Qatar' => {
	'' => 121,
    },
    'Ecuador' => {
        '' => 125,
    },
    'Czechia' => {
        '' => 126,
    },
    'Armenia' =>  {
        '' => 127,
    },
    'Dominican Republic' => {
        '' => 128,
    },
    'Indonesia' => {
	'' => 131,
    },
    'Portugal' => {
	'' => 132,
    },
    'Andorra' => {
	'' => 133,
    },
    'Latvia' => {
	'' => 135,
    },
    'Morocco' => {
        '' => 136,
    },
    'Saudi Arabia' => {
        '' => 137,
    },
    'Senegal' => {
	'' => 138,
    },
    'Argentina' => {
        '' => 149,
    },
    'Chile' => {
        '' => 150,
    },
    'Jordan' => {
        '' => 151,
    },
    'Ukraine' => {
	'' => 157,
    },
    'Hungary' => {
        '' => 159,
    },
    'Liechtenstein' => {
	'' => 163,
    },
    'Poland' => {
	'' => 164,
    },
    'Tunisia' => {
	'' => 165,
    },
    'West Bank and Gaza' => {
        '' => 167,
    },
    'Bosnia and Herzegovina' => {
        '' => 168,
    },
    'Slovenia' => {
        '' => 169,
    },
    'South Africa' => {
        '' => 173,
    },
    'Bhutan' => {
	'' => 207,
    },
    'Cameroon' => {
	'' => 187,
    },
    'Colombia' => {
	'' => 188,
    },
    'Costa Rica' => {
        '' => 189,
    },
    'Peru' => {
	'' => 190,
    },
    'Serbia' => {
	'' => 191,
    },
    'Slovakia' => {
        '' => 192,
    },
    'Togo' => {
        '' => 193,
    },
    # 'French Guiana' => {
    #    '' => 212,
    # },
    'Malta' => {
	'' => 213,
    },
    # 'Martinique' => {
    #     '' => 216,
    # },
    'Bulgaria' => {
        '' => 242,
    },
    'Maldives' => {
        '' => 243,
    },
    'Bangladesh' => {
        '' => 245,
    },
    'Moldova' => {
        '' => 249,
    },
    'Paraguay' => {
	'' => 250,
    },
    'Albania' => {
        '' => 274,
    },
    'Cyprus' => {
        '' => 275,
    },
    'Brunei' => {
        '' => 278,
    },
    'Burkina Faso' => {
	'' => 328,
    },
    'Holy See' => {
	'' => 330,
    },
    'Mongolia' => {
	'' => 331,
    },
    'Panama' => {
	'' => 332,
    },
    'Denmark' => {
	'' => 104,
	'Faroe Islands' => 161,
	'Greenland' => 504,    
    },
    'Bolivia' => {
	'' => 392,
    },
    'Honduras' => {
        '' => 394,
    },
    'Congo (Kinshasa)' => {
        '' => 396,
    },
    'Cote d\'Ivoire' => {
	'' => 397,
    },
    'Jamaica' => {
        '' => 398,
    },
    'Turkey' => {
        '' => 400,
    },
    'Cuba' => {
        '' => 454,
    },
    'Guyana' => {
        '' => 455,
    },
    'Kazakhstan' => {
        '' => 457,
    },
    # 'Guadeloupe' => {
    #     '' => 460,
    # },
    'Ethiopia' => {
        '' => 463,
    },
    'Sudan' => {
        '' => 464,
    },
    'Guinea' => {
	'' => 465,
    },
    'Kenya' => {
        '' => 467,
    },
    'Antigua and Barbuda' => {
        ''=> 469,
    },
    'Uruguay' => {
        '' => 472,
    },
    'Ghana' => {
        '' => 473,
    },
    'Namibia' => {
        '' => 476,
    },
    'Seychelles' => {
        '' => 477,
    },
    'Trinidad and Tobago' => {
        '' => 478,
    },
    'Venezuela' => {
        '' => 479,
    },
    'Eswatini' => {
        '' => 481,
    },
    'Gabon' => {
        '' => 482,
    },
    'Guatemala' => {
        '' => 483,
    },
    'Mauritania' => {
        '' => 485,
    },
    'Rwanda' => {
        '' => 486,
    },
    'Saint Lucia' => {
        '' => 487,
    },
    'Saint Vincent and the Grenadines' => {
        '' => 488,
    },
    'Suriname' => {
        '' => 489,
    },
    'Kosovo' => {
        '' => 492,
    },
    'Central African Republic' => {
        '' => 495,
    },
    'Congo (Brazzaville)' => {
        '' => 496,
    },
    'Equatorial Guinea' => {
        '' => 497,
    },
    'Uzbekistan' => {
        '' => 499,
    },
    'Benin' => {
        '' => 503,
    },
    'Liberia' => {
        '' => 505,
    },
    'Somalia' => {
        '' => 509,
    },
    'Tanzania' => {
        '' => 510,
    },
    'Barbados' => {
	'' => 515,
    },
    'Montenegro' => {
        '' => 516,
    },
    'Gambia' => {
        '' => 517,
    },
    'Kyrgyzstan' => {
        '' => 518,
    },
    'Mauritius' => {
        '' => 519,
    },
    'Zambia' => {
        '' => 521,
    },
    'Djibouti' => {
        '' => 522,
    },
    'Chad' => {
        '' => 526,
    },
    'El Salvador' => {
        '' => 527,
    },
    'Fiji' => {
        '' => 528,
    },
    'Nicaragua' => {
        '' => 529,
    },
    'Madagascar' => {
        '' => 530,
    },
    'Haiti' => {
        '' => 531,
    },
    'Angola' => {
        '' => 532,
    },
    'Cabo Verde' => {
        '' => 533,
    },
    'Niger' => {
        '' => 535,
    },
    'Papua New Guinea' => {
        '' => 536,
    },
    'Zimbabwe' => {
        '' => 538,
    },
    # 'Cape Verde' => {
    #     '' => 540,
    # },
    # 'East Timor' => {
    #     '' => 541,
    # },
    'Eritrea' => {
        '' => 542,
    },
    'Uganda' => {
        '' => 543,
    },
    'Dominica' => {
        '' => 544,
    },
    'Grenada' => {
        '' => 545,
    },
    'Mozambique' => {
        '' => 546,
    },
    'Syria' => {
        '' => 547,
    },
    'Timor-Leste' => {
        '' => 548,
    },
    # 'Reunion' => {
    #     '' => 556,
    # },
    # 'Mayotte' => {
    #     '' => 507,
    # },
    # 'Greenland' => {
    #     '' => 554,
    # },
    # 'Guam' => {
    #     '' => 491,
    # },
    # 'Puerto Rico' => {
    #     '' => 514,
    # },
    # 'Republic of the Congo' => {
    #     '' => 508,
    # },
    'Bahamas' => {
        '' => 511,
    },
    'Diamond Princess' => {
        '' => 73,
    },
    'Belize' => {
        '' => 562,
    },
    'Laos' => {
        '' => 563,
    },
    'Libya' => {
        '' => 564,
    },
    'Guinea-Bissau' => {
        '' => 566,
    },
    'Mali' => {
        '' => 567,
    },
    'Saint Kitts and Nevis' => {
        '' => 568,
    },
    'Burma' => {
        '' => 572,
    },
    'MS Zaandam' => {
        '' => 576,
    },
    'Botswana' => {
        '' => 577,
    },	       
    'Burundi' => {
        '' => 578,
    },
    'Sierra Leone' => {
        '' => 579,
    },
    'Malawi' => {
        '' => 581,
    },
    'South Sudan' => {
        '' => 584,
    },
    'Western Sahara' => {
        '' => 585,
    },
    'Sao Tome and Principe' => {
        '' => 586,
    },
    'Yemen' => {
	'' => 587,
    },
    'Comoros' => {
	'' => 588,
    },
    'Tajikistan' => {
        '' => 589,
    },
};
    
# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'corona-virus';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($url, $csv_prefix, $csv_suffix, %sheets);

my ($places, $observations) = extract_data(%sheets);

verify_places_map($place_id_map, $observations);

import_data($places, $observations);

publish($url);

sub download_files {
    my $url_base   = shift;
    my $csv_prefix = shift;
    my $csv_suffix = shift;
    my %sheets     = @_;


    foreach my $sheet ( keys %sheets ) {
	my $url = $url_base . $csv_prefix . $sheet . $csv_suffix;
	run_command("curl -f -L -o $sheet.csv '$url'",
		    "Could not download $url");
    }
}

sub extract_data {
    my %sheets = @_;

    my $places = {};
    my $observations = {};
    
    foreach my $sheet ( keys %sheets ) {
	my $csv = "$sheet.csv";

	my $data = csv(in => $csv);

	die "No data in $csv" unless @{$data};

	my @header = @{shift @{$data}};
	# Everything after column 4 is observations, so we'll grab those
	# as timestamps. This used to be column 5 but they removed first
	# confirmed date.
	my @timestamps = splice(@header, 4);

	# Build the places and $observations hashes from the data
	foreach my $row ( @{$data} ) {
	    # Remove leading spaces
	    $row->[0] =~ s/^\s+//g;
	    $row->[1] =~ s/^\s+//g;

	    # Remove trailing spaces
	    $row->[0] =~ s/\s+$//g;
            $row->[1] =~ s/\s+$//g;
	    
	    my $place_id = calculate_place_id($row->[1], $row->[0]);
	    $places->{$place_id}{'country'} = $row->[1];
	    $places->{$place_id}{'state'}   = $row->[0];
	    $places->{$place_id}{'lat'}     = sprintf("%.4f", $row->[2]);
	    $places->{$place_id}{'long'}    = sprintf("%.4f", $row->[3]);

	    my $current = '';
	    my $i = 0; # Use this to look up the timestamp
	    foreach my $observation ( splice @{$row}, 4 ) {
		if ( $observation eq '' or $observation eq $current) {
		    $i++;
		    next;
		}
		$current = $observation;
		die "Empty string in $current for $sheet, $place_id, $timestamps[$i]" if ( $current eq '' );
		my $date_time = convert_timestamp($timestamps[$i]);
		$observations->{$place_id}{$date_time}{lc($sheets{$sheet})} =
		    $observation;
		$i++;
	    }
	}
    }

    return ($places, $observations);
}

sub verify_places_map {
    my $places_map  = shift;
    my $places_used = shift;

    # No dupes
    my %dedupe= ();
    foreach my $country ( keys %{$places_map} ) {
	foreach my $state ( keys %{$places_map->{$country}} ) {
	    my $id = $places_map->{$country}{$state};
	    die "Duplicate found for $id" if $dedupe{$id};
	    $dedupe{$id} = 1;

	    die "Unused place id: $id, $country, $state"
		unless $places_used->{$id};
	}
    }
}

sub calculate_place_id {
    my $country = shift;
    my $state   = shift;
    
    # I think I just use a map here with country and state as keys.
    # If I get back undefined, error becuase I'm not sure if they found cases
    # in a new place or whether they updated an existing place.
    my $place_id = $place_id_map->{$country}{$state};

    die "Could not determine place_id for country: '$country' and state: '$state'" unless $place_id;

    return $place_id;
}

sub import_data {
    my $places       = shift;
    my $observations = shift;

    import_places($places);
    import_observations($observations);
}

sub import_places {
    my $places = shift;

    open PLACES_SQL, '>places.sql' or die "Could not open places.sql\n";
    print PLACES_SQL "delete from places;\n";
    
    foreach my $place_id ( keys %{$places} ) {
	my $country = escape_sql($places->{$place_id}{'country'});
	my $state   = escape_sql($places->{$place_id}{'state'});
	my $lat     = $places->{$place_id}{'lat'};
	my $long    = $places->{$place_id}{'long'};

	my $sql =
	    "insert into places (place_id, country_region, province_state, latitude, longitude) values ($place_id, '$country', '$state', $lat, $long);\n";
	print PLACES_SQL $sql;
    }
    close PLACES_SQL;
    
    run_command("dolt sql < places.sql", "Could not execute places.sql");
}

sub import_observations {
    my $observations = shift;

    open OBS_SQL, '>obs.sql' or die "Could not open obs.sql\n";
    print OBS_SQL "delete from cases;\n";

    foreach my $place_id ( keys %{$observations} ) {
	foreach my $date ( keys %{$observations->{$place_id}} ) {
	    my $sql =
		"insert into cases (place_id, observation_time) values ($place_id, '$date');\n";
	    print OBS_SQL $sql;
	    
	    my $cases = $observations->{$place_id}{$date};
	    die "No cases found for $place_id, $date" if ( !%{$cases} );
	    foreach my $type ( keys %{$cases} ) {
		my $count = $cases->{$type};
		my $column_name = $type . "_count";
		$sql = "update cases set $column_name = $count where place_id=$place_id and observation_time='$date';\n";
		print OBS_SQL $sql;
	    }
	}
    }
    close OBS_SQL;

    run_command("dolt sql < obs.sql", "Could not execute obs.sql");
}

sub convert_timestamp {
    my $timestamp = shift;

    my $month;
    my $day;
    my $year;
    if ( $timestamp =~ /^(\d+)\/(\d+)\/(\d+)$/ ) {
	$month  = $1;
	$day    = $2;
	$year   = $3;
    } else {
	die "Timestamp does not match expected format";
    }
	
    $day   = "0$day" if ( $day < 10 and $day =~ /^\d$/);
    $month = "0$month" if ( $month < 10 and $month =~ /^\d$/);
    $year  = "20$year" if ( $year == 20 );
    
    my $date_time = "$year-$month-$day 00:00:00";

    return $date_time;
}

sub publish {
    my $url = shift;

    unless ( `dolt diff` ) {
        print "Nothing changed in import. Not generating a commit\n";
        exit 0;
    }

    run_command('dolt add .', 'dolt add command failed');

    my $datestring = gmtime();
    my $commit_message =
 	"Automated import of cases and places tables downloaded from $url at $datestring GMT";

    run_command('dolt commit -m "' . $commit_message . '"', 
                "dolt commit failed");

    run_command('dolt pull', 'Could not pull latest master');

    if ( `dolt status` =~ /merging/ ) {
	run_command('dolt commit -m "Merging latest master"',
		    'dolt commit failed');
    }
    
    run_command('dolt push origin master', 'dolt push failed');
}

sub escape_sql {
    my $string = shift;

    $string =~ s/\\/\\\\/g;
    $string =~ s/'/\\'/g;
    
    return $string;
}

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
