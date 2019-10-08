/*
 * Copyright (c) 2019 Martijn Heil
 * Alle rechten voorbehouden.
 */

extern crate reqwest;
extern crate json;
extern crate clap;
extern crate pbr;
extern crate tee_readwrite;

use std::thread::sleep;
use std::time::Duration;
use std::fs::File;
use std::fs;
use std::io::stderr;
use std::fmt::Display;

use clap::{app_from_crate, crate_name, crate_version, crate_authors, crate_description};
use clap::Arg;

use reqwest::StatusCode;

use json::object;
use json::JsonValue;

use pbr::{ProgressBar, Units};
use tee_readwrite::TeeWriter;


#[derive(Debug)]
struct UnexpectedStatusCodeError {
  response: reqwest::Response,
  response_text: Option<String>,
  method: reqwest::Method,
}

impl UnexpectedStatusCodeError {
  fn new(mut response: reqwest::Response, method: reqwest::Method) -> Self {
    let response_text = response.text().ok();
    Self { response, response_text, method }
  }
}

impl std::error::Error for UnexpectedStatusCodeError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    None
  }
}

impl Display for UnexpectedStatusCodeError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "Onverwachte status code ({}) gekregen als antwoord op {} {}\n",
        self.response.status(), self.method, self.response.url())?;
    if let Some(text) = &self.response_text {
      write!(f, "De PDOK API zegt:\n{}", text)?;
    }
    Ok(())
  }
}

fn main() {
  std::process::exit(match run_app() {
    Err(err) => {
      eprintln!("Error: {}", err);
      1
    }
    Ok(_) => 0
  })
}

fn run_app() -> Result<(), Box<dyn std::error::Error>> {
  let user_agent = format!("DKKdownload v{}", env!("CARGO_PKG_VERSION"));

  let matches = app_from_crate!()
    .arg(Arg::with_name("boundingpolygon")
      .value_name("BOUNDINGPOLYGON")
      .help("Bounding Well-Known Text (WKT) polygon")
      .required(true)
      .index(1))
    .arg(Arg::with_name("output_file")
      .value_name("FILE")
      .short("o")
      .long("output")
      .takes_value(true)
      .help("Pad naar output ZIP-bestand. Bijvoorbeeld: 'output.zip'. Wanneer dit ongespecificeerd wordt gelaten zal het ZIP-bestand naar stdout worden geschreven."))
    .arg(Arg::with_name("bounding_polygon_is_file")
      .short("f")
      .long("file")
      .help("Interpreteer BOUNDINGPOLYGON als pad naar WKT bestand i.p.v. als WKT string."))
    .arg(Arg::with_name("lagen")
      .value_name("LAGEN")
      .help("Lijst van lagen om te downloaden, met een spatie tussen elke laag.")
      .multiple(true)
      .index(2)
      .required(true))
    .arg(Arg::with_name("progress")
        .short("p")
        .long("progress")
        .help("Geef voortgang weer in stderr."))
    .about("Copyright (c) 2019 Martijn Heil\n\
        Gebruik van dit programma is uitsluitend voorbehouden aan gemeente Lingewaard.\n\
        \nProgramma om de Digitale Kadastrale Kaart (DKK) in vector-formaat te downloaden - gefilterd met een bounding polygon - d.m.v. de PDOK DKK Download API.")
    .get_matches();

  let show_progress = matches.is_present("progress");

  let bpf = matches.value_of("boundingpolygon").expect("BOUNDINGPOLYGON mag niet leeg zijn.");
  let interessegebied: String = match matches.is_present("bounding_polygon_is_file") { // Well-Known Text (WKT) polygon string
    true => {
      fs::read_to_string(bpf)?
    },
    false => {
      String::from(bpf)
    }
  };
  let layers: Vec<&str> = matches.values_of("lagen").expect("Er moet minimaal 1 laag gespecificeerd worden.").collect();

  let probing_interval: Duration = Duration::from_millis(1000);

  let output_filepath = matches.value_of("output_file");

  let mut output_writer: Box<dyn std::io::Write> = match output_filepath {
    Some(path) => {
      Box::new(File::create(path)?)
    },
    None => {
      Box::new(std::io::stdout())
    }
  };

  let root_url = "https://downloads.pdok.nl";
  let root_api_url = format!("{}{}", root_url, "/kadastralekaart/api/v4_0");

  let client = reqwest::Client::new();

  // featuretypes kan bijv. het volgende zijn;
  // array![
  //    "perceel",
  //    "kadastralegrens",
  //    "pand",
  //    "openbareruimtelabel"
  //  ],
  let body = object!{
    "featuretypes" => JsonValue::from(layers),
    "format" => "gml", // "gml" is per najaar 2019 ook de enige toegestane waarde.
    "geofilter" => interessegebied
  };
  let requrl = format!("{}{}", root_api_url, "/full/custom");
  let jsonbody = json::stringify(body.clone());
  let mut res = client.post(requrl.as_str())
    .header(reqwest::header::USER_AGENT, &user_agent)
    .header(reqwest::header::ACCEPT, "application/json")
    .header(reqwest::header::CONTENT_TYPE, "application/json") // Als je deze niet zend, zend de PDOK API een 500tje terug: stand 2019-10-2
    .body(jsonbody)
    .send()?;

  if res.status() != StatusCode::ACCEPTED {
    return Err(Box::new(UnexpectedStatusCodeError::new(res, reqwest::Method::POST)));
  }

  let restext = res.text()?;
  let resjson = json::parse(&restext)?;

  let reqid: &str = resjson["downloadRequestId"].as_str().expect("Verkregen downloadRequestId van de PDOK API is geen string.");

  let mut progress_foreign = None;

  if show_progress {
    progress_foreign = Some(ProgressBar::on(stderr(), 100));
    progress_foreign.as_mut().unwrap().message("PDOK API is bezig met processen ");
    progress_foreign.as_mut().unwrap().show_tick = true;
  }

  loop {
    let status_url = format!("{}{}{}/status", root_api_url, "/full/custom/", reqid);
    let mut res = client.get(status_url.as_str())
      .header(reqwest::header::USER_AGENT, &user_agent)
      .header(reqwest::header::ACCEPT, "application/json")
      .send()?;
    match res.status() { // "Full custom download nog niet gereed"
      StatusCode::OK => {
        if show_progress {
          progress_foreign.as_mut().unwrap().tick();

          let restext = res.text();
          match restext {
            Ok(text) => {
              let resjson = json::parse(&text);
              match resjson {
                Ok(statusjson) => {
                  let progress = statusjson["progress"].as_u64();
                  if let Some(progress) = progress {
                    progress_foreign.as_mut().unwrap().set(progress);
                  }
                }
                Err(_) => {} // Niks doen
              }
            }
            Err(_) => {} // Niks doen
          }
        }

        sleep(probing_interval);
        continue;
      }
      StatusCode::CREATED => {
        if show_progress {
          progress_foreign.as_mut().unwrap().finish();
        }

        let restext = res.text()?;
        let resjson = json::parse(&restext)?;
        let download_url = format!("{}{}", root_url, resjson["_links"]["download"]["href"]);

        // Download url verwijst naar een zip bestand
        let mut zipfileres = client.get(download_url.as_str())
          .header(reqwest::header::USER_AGENT, &user_agent)
          .header(reqwest::header::ACCEPT, "application/json")
          .send()?;
        match zipfileres.status() {
          StatusCode::OK => {
            if show_progress {
              if let Some(length) = zipfileres.content_length() {
                let old_output_writer = output_writer;
                let mut progress_own = ProgressBar::on(stderr(), length);
                progress_own.message("ZIP bestand downloaden ");
                progress_own.set_units(Units::Bytes);
                output_writer = Box::new(TeeWriter::new(old_output_writer, progress_own));
                zipfileres.copy_to(&mut output_writer)?;
                return Ok(());
              }
            }

            zipfileres.copy_to(&mut output_writer)?;
            return Ok(());
          },
          _ => {
            return Err(Box::new(UnexpectedStatusCodeError::new(zipfileres, reqwest::Method::GET)));
          }
        }
      },
      _ => { return Err(Box::new(UnexpectedStatusCodeError::new(res, reqwest::Method::GET))); }
    }
  }
}
