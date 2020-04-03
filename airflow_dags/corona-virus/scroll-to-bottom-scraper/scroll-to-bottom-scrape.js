const fs = require('fs');
const puppeteer = require('puppeteer');

let url  = process.argv[2];
let file = process.argv[3];

if ( !url || !file ) {
    console.log("Must define a url and file as arguments");
    process.exit(1);
}

(async () => {
    // Set up browser and page.
    const browser = await puppeteer.launch({
	args: ['--no-sandbox', '--disable-setuid-sandbox'],
	headless: false,
    });
    const page = await browser.newPage();
    page.setViewport({ width: 1280, height: 926 });
    
    // Navigate to the demo page.
    console.log("Loading " + url);
    await page.goto(url);

    console.log("Scrolling to the bottom of the page. Please wait.");
    await autoScroll(page);
    
    // Save extracted items to a file.
    const html = await page.content();
    console.log("Writing HTML to " + file);
    fs.writeFileSync(file, html);
		    
    // Close the browser.
    await browser.close();
})();

async function autoScroll(page){
    await page.evaluate(async () => {
        await new Promise((resolve, reject) => {
            var totalHeight = 0;
            var distance = 5000;
	    var scrolls = 0;
	    
            var timer = setInterval(() => {
                var scrollHeight = document.body.scrollHeight;
                window.scrollBy(0, distance);
		totalHeight += distance;
		scrolls++;
		
                // if(totalHeight >= scrollHeight){
		if ( scrolls > 300 ) {
                    clearInterval(timer);
                    resolve();
                }
            }, 1000);
        });
    });
}
