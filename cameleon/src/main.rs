use cameleon::u3v;
use std::sync::mpsc::channel;

fn main() {
    // Enumerates all cameras connected to the host.
    let mut cameras = u3v::enumerate_cameras().unwrap();

    if cameras.is_empty() {
        println!("no camera found");
        return;
    }

    let (tx, rx) = channel();

    for (index, mut camera) in cameras.into_iter().enumerate() {
        let tx = tx.clone();
        std::thread::spawn(move || {
            // Opens the camera.
            camera.open().unwrap();
            // Loads `GenApi` context. This is necessary for streaming.
            camera.load_context().unwrap();

            // Start streaming. Channel capacity is set to 3.
            let payload_rx = camera.start_streaming(3).unwrap();

            let start = std::time::Instant::now();

            let mut payload_count = 0;
            loop {
                match payload_rx.try_recv() {
                    Ok(payload) => {
                        // println!(
                        //     "payload received! block_id: {:?}, timestamp: {:?}",
                        //     payload.id(),
                        //     payload.timestamp()
                        // );
                        if let Some(image_info) = payload.image_info() {
                            println!("{:?}\n", image_info);
                            let image = payload.image();
                            // do something with the image.
                            // ...
                        }
                        payload_count += 1;
                        let elapsed = start.elapsed().as_secs_f32();
                        let fps = payload_count as f32 / elapsed;
                        tx.send((index, fps)).unwrap();

                        // Send back payload to streaming loop to reuse the buffer. This is optional.
                        payload_rx.send_back(payload);
                    }
                    Err(_err) => {
                        continue;
                    }
                }
            }
        });
    }

    loop {
        match rx.recv() {
            Ok((index, fps)) => {
                println!("Camera {}: Overall fps: {:.2}", index, fps);
            }
            Err(_) => break,
        }
    }
}
