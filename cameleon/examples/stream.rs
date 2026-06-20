/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! This example describes how to start streaming and receive payloads.

use std::sync::mpsc::Sender;
use std::thread;
use std::{net::Ipv4Addr, time::Instant};

use cameleon::gige::enumerate_cameras;

use chrono::Utc;
use ljpeg::Encoder;
use minifb::{Key, KeyRepeat, Window, WindowOptions};
use mlv::{frame::RawInfo, Fraction};

use std::fs::File;
use std::io::{BufWriter, Write};

const WIDTH: usize = 2048;
const HEIGHT: usize = 1088;

fn write_mlv() -> Sender<Vec<u8>> {
    let (send, recv) = std::sync::mpsc::channel::<Vec<u8>>();
    let mut mlv = mlv::FileHeader::default();
    mlv.video_class |= mlv::VideoClass::Raw as u16;
    mlv.video_class |= mlv::VideoClassFlag::LJ92 as u16;
    mlv.fps = mlv::Fraction::integral(24);
    mlv.video_frame_count = 1;

    dbg!(mlv.video_class);

    let raw_info = RawInfo {
        res_x: 2048,
        res_y: 1088,
        api_version: 1,
        buffer: 0,
        height: 1088,
        width: 2048,
        pitch: 12,
        frame_size: 2048 * 1088,
        bits_per_pixel: 12,
        black_level: 0,
        white_level: 2 << 12,
        origin: [0, 0],
        size: [2048, 1088],
        dng_active_area: [0; 4],
        exposure_bias: [0, 2],
        cfa_pattern: 0,
        calibration_illuminant: 0,
        color_matrix: [
            Fraction::new(1, 1),
            Fraction::new(0, 1),
            Fraction::new(0, 1),
            Fraction::new(0, 1),
            Fraction::new(1, 1),
            Fraction::new(0, 1),
            Fraction::new(0, 1),
            Fraction::new(0, 1),
            Fraction::new(1, 1),
        ],
        dynamic_range: 12,
    };
    let frame = mlv::frame::Frame {
        header: raw_info.into(),
        payload: vec![],
    };

    let encoder = Encoder::new(
        2048,
        1088,
        ljpeg::Components::C1,
        ljpeg::Bitdepth::B12,
        ljpeg::Predictor::P7,
        0,
        0,
    );

    thread::spawn(move || {
        let iterator = (0..)
            .map(|_| {
                let payload = match recv.recv() {
                    Ok(payload) => payload,
                    Err(e) => {
                        println!("payload receive error: {e}");
                        return None;
                    }
                };
                let p: &[u16] = bytemuck::cast_slice(&payload);

                let time = Instant::now();
                let comp = encoder.encode(&p).unwrap();
                // ljpeg::Decoder::new(&comp).unwrap().decode().unwrap();
                println!(
                    "compressed: {} {}ms",
                    comp.len(),
                    time.elapsed().as_micros()
                );
                Some(comp)
            })
            .take_while(Option::is_some)
            .flat_map(|i| i);

        let now = Utc::now();
        let file = File::create(format!("P{}.MLV", now.format("%d-%H%M%S"))).unwrap();
        let mut file = BufWriter::new(file);
        mlv.write_mlv(
            vec![frame],
            iterator,
            std::iter::empty::<Vec<u8>>(),
            &mut file,
        )
        .unwrap();
        file.flush().unwrap();
        println!("file written");
    });
    send
}

fn main() {
    let mut buffer: Vec<u32> = vec![0; WIDTH * HEIGHT];
    let mut window = Window::new(
        "Test - ESC to exit",
        WIDTH,
        HEIGHT,
        WindowOptions::default(),
    )
    .unwrap_or_else(|e| {
        panic!("{}", e);
    });

    // Limit to max ~60 fps update rate
    window.set_target_fps(60);

    // Enumerates cameras connected to the host.
    let mut cameras = enumerate_cameras(Ipv4Addr::new(169, 254, 247, 66)).unwrap();

    if cameras.is_empty() {
        println!("no camera found!");
        return;
    }
    println!("Found {} cameras", cameras.len());

    let mut camera = cameras.pop().unwrap();

    // Open the camera.
    camera.open().unwrap();
    // Load `GenApi` context.
    camera.load_context().unwrap();
    {
        let mut ctxt = camera.params_ctxt().unwrap();
        ctxt.node("PixelFormat")
            .unwrap()
            .as_enumeration(&ctxt)
            .unwrap()
            .set_entry_by_symbolic(&mut ctxt, "Mono12")
            .unwrap();

        let entries = ctxt
            .node("ExposureMode")
            .unwrap()
            .as_enumeration(&ctxt)
            .unwrap()
            .set_entry_by_symbolic(&mut ctxt, "PieceWiseLinearHDR")
            .unwrap();

        let fps = ctxt
            .node("AcquisitionFrameRateAbs")
            .unwrap()
            .as_float(&ctxt)
            .unwrap()
            .set_value(&mut ctxt, 24.0)
            .unwrap();

        let exp = ctxt
            .node("ExposureTimeAbs")
            .unwrap()
            .as_float(&ctxt)
            .unwrap()
            .set_value(&mut ctxt, 20833.3)
            .unwrap();

        let th1 = ctxt
            .node("ThresholdPWL1")
            .unwrap()
            .as_integer(&ctxt)
            .unwrap()
            .set_value(&mut ctxt, 48)
            .unwrap();

        let th2 = ctxt
            .node("ThresholdPWL2")
            .unwrap()
            .as_integer(&ctxt)
            .unwrap()
            .set_value(&mut ctxt, 32)
            .unwrap();

        let exp1 = ctxt
            .node("ExposureTimePWL1")
            .unwrap()
            .as_float(&ctxt)
            .unwrap()
            .set_value(&mut ctxt, 10000.0)
            .unwrap();
        let exp2 = ctxt
            .node("ExposureTimePWL2")
            .unwrap()
            .as_float(&ctxt)
            .unwrap()
            .set_value(&mut ctxt, 4000.0)
            .unwrap();

        // println!("th1: {th1:?}, exp1: {exp1}");
        // println!("th2: {th2:?}, exp2: {exp2}");
    }

    // Start streaming. Channel capacity is set to 3.
    let payload_rx = camera.start_streaming(10).unwrap();
    println!("streaming started");

    let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
    compressor.multithread(4).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(500));

    let mut send = None;

    while window.is_open() && !window.is_key_down(Key::Escape) {
        if window.is_key_pressed(Key::R, KeyRepeat::No) {
            match send {
                None => send = Some(write_mlv()),
                Some(_) => send = None,
            }
        }
        let payload = match payload_rx.recv_blocking() {
            Ok(payload) => payload,
            Err(e) => {
                println!("payload receive error: {e}");
                continue;
            }
        };
        // let image_info = payload.image_info()?;
        // println!("{image_info:?}\n");
        let payload = payload.into_vec();
        let p: &[u16] = bytemuck::cast_slice(&payload);
        for (i, b) in p.iter().enumerate() {
            let b = *b as u32 >> 4;
            buffer[i] = b << 16 | b << 8 | b;
        }
        window.update_with_buffer(&buffer, WIDTH, HEIGHT).unwrap();

        // let mut test = vec![0; p.len()];
        // let mut a = p[0];
        // test[0] = a as i16;
        // for (i, &x) in p.iter().enumerate().skip(1) {
        //     test[i] = x as i16 - a as i16;
        //     a = x;
        // }
        // let t = bytemuck::cast_slice(&test);

        // let time = Instant::now();
        // let comp = encoder.encode(p).unwrap();
        // ljpeg::Decoder::new(&comp).unwrap().decode().unwrap();
        // println!(
        //     "compressed: {} {}ms",
        //     comp.len(),
        //     time.elapsed().as_micros()
        // );
        // write_pgm(
        //     &format!("test{i}.pgm"),
        //     payload.payload(),
        //     image_info.width,
        //     image_info.height,
        // );

        // Send back payload to streaming loop to reuse the buffer.
        if let Some(ref s) = send {
            s.send(payload);
        }
        // payload_rx.send_back(payload);
    }

    camera.stop_streaming().unwrap();

    camera.close().ok();
}

fn write_pgm(name: &str, data: &[u8], width: usize, height: usize) {
    let file = File::create(name).unwrap();
    let mut file = BufWriter::new(file);
    writeln!(&mut file, "P5 {} {} 255", width, height).unwrap();
    file.write_all(data).unwrap();
}
