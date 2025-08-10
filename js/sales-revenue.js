document.addEventListener('DOMContentLoaded', () => {
    // API endpoint'lerini burada tanımlayacağız
    const API_ENDPOINTS = {
        metrics: 'https://api.example.com/dashboard/metrics', // Örnek API URL'i
        revenueTrend: 'https://api.example.com/dashboard/revenue-trend',
        salesByChannel: 'https://api.example.com/dashboard/sales-by-channel',
        topSellingProducts: 'https://api.example.com/dashboard/top-selling-products'
    };

    // Chart.js grafikleri için canvas elementleri
    const revenueTrendCtx = document.getElementById('revenueTrendChart').getContext('2d');
    const salesByChannelCtx = document.getElementById('salesByChannelChart').getContext('2d');

    // Chart.js grafikleri için boş değişkenler
    let revenueTrendChart;
    let salesByChannelChart;

    // Renk paleti (CSS'deki ile tutarlı, Chart.js için)
    const CHART_COLORS = {
        primaryPurple: '#886FE4',
        secondaryPurple: '#E0B8FF',
        blueAccent: '#6495ED',
        redAccent: '#FF6384',
        textLight: '#F0F0F0',
        textMuted: '#A0A0A0',
        gridColor: 'rgba(255, 255, 255, 0.1)',
        cardBg: '#0F3460' // Kart arka plan rengi
    };

    /**
     * API'den veri çekme fonksiyonu
     * @param {string} url - API endpoint URL'si
     * @returns {Promise<Object>} - Çekilen veri
     */
    async function fetchData(url) {
        try {
            // Gerçek API entegrasyonu için aşağıdaki yorum satırını kaldırın ve MOCK_API_RESPONSES kısmını kaldırın
            // const response = await fetch(url);
            // if (!response.ok) {
            //     throw new Error(`HTTP error! status: ${response.status}`);
            // }
            // return await response.json();

            // Sadece test için mock verileri kullan
            console.log(`Fetching mock data for: ${url}`);
            const key = Object.keys(MOCK_API_RESPONSES).find(mockUrl => url.includes(mockUrl));
            if (key) {
                return new Promise(resolve => setTimeout(() => resolve(MOCK_API_RESPONSES[key]), 500)); // 500ms gecikme
            }
            return null; // Eğer mock veri yoksa
        } catch (error) {
            console.error("Veri çekme hatası:", error);
            return null;
        }
    }

    /**
     * Üst metrikleri doldurma fonksiyonu
     * @param {Object} data - Metrik verileri
     */
    function updateMetrics(data) {
        if (data) {
            document.getElementById('totalRevenue').textContent = data.totalRevenue || '$2.4M';
            document.getElementById('conversionRate').textContent = data.conversionRate || '3.47%';
            document.getElementById('averageBasket').textContent = data.averageBasket || '₺347';
            document.getElementById('totalOrders').textContent = data.totalOrders || '6,847';

            // Yüzdesel değişimler için span'leri de güncelleyebilirsiniz, API'den gelirse
            // const totalRevenueBadge = document.getElementById('totalRevenue').nextElementSibling;
            // if (data.totalRevenueChange) {
            //     totalRevenueBadge.textContent = `${data.totalRevenueChange > 0 ? '+' : ''}${data.totalRevenueChange}%`;
            //     totalRevenueBadge.classList.remove('bg-danger-gradient'); // Önceki sınıfları kaldır
            //     totalRevenueBadge.classList.add('bg-success-gradient'); // Yeni sınıfı ekle
            // }
        }
    }

    /**
     * Gelir Trendi grafiğini oluşturma/güncelleme fonksiyonu
     * @param {Object} data - Gelir trendi verileri
     */
    function updateRevenueTrendChart(data) {
        if (data && data.labels && data.values) {
            if (revenueTrendChart) {
                revenueTrendChart.destroy();
            }
            revenueTrendChart = new Chart(revenueTrendCtx, {
                type: 'line',
                data: {
                    labels: data.labels,
                    datasets: [{
                        label: 'Toplam Gelir',
                        data: data.values,
                        borderColor: CHART_COLORS.primaryPurple, // Ana mor
                        backgroundColor: 'rgba(136, 111, 228, 0.2)', // Açık mor dolgu
                        tension: 0.4, // Daha yumuşak eğri
                        fill: true,
                        pointBackgroundColor: CHART_COLORS.primaryPurple,
                        pointBorderColor: CHART_COLORS.textLight,
                        pointBorderWidth: 2,
                        pointRadius: 5,
                        pointHoverRadius: 7
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: {
                            grid: {
                                color: CHART_COLORS.gridColor,
                                drawBorder: false
                            },
                            ticks: {
                                color: CHART_COLORS.textMuted,
                                font: {
                                    size: 12
                                }
                            }
                        },
                        y: {
                            beginAtZero: true,
                            grid: {
                                color: CHART_COLORS.gridColor,
                                drawBorder: false
                            },
                            ticks: {
                                color: CHART_COLORS.textMuted,
                                font: {
                                    size: 12
                                },
                                callback: function(value) {
                                    return '₺' + (value / 1000) + 'K';
                                }
                            }
                        }
                    },
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            backgroundColor: CHART_COLORS.cardBg, // Kart arka plan rengiyle uyumlu
                            titleColor: CHART_COLORS.textLight,
                            bodyColor: CHART_COLORS.textLight,
                            borderColor: CHART_COLORS.primaryPurple,
                            borderWidth: 1,
                            cornerRadius: 6,
                            padding: 10,
                            callbacks: {
                                label: function(context) {
                                    let label = context.dataset.label || '';
                                    if (label) {
                                        label += ': ';
                                    }
                                    if (context.parsed.y !== null) {
                                        label += '₺' + context.parsed.y.toLocaleString('tr-TR');
                                    }
                                    return label;
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    /**
     * Kanallara Göre Satış grafiğini oluşturma/güncelleme fonksiyonu
     * @param {Object} data - Kanallara göre satış verileri
     */
    function updateSalesByChannelChart(data) {
        if (data && data.labels && data.values) {
            // Görseldeki renkleri yansıtacak şekilde renk paleti
            const channelColors = [
                '#886FE4', // Ana mor
                '#E0B8FF', // Açık mor
                '#6A5ACD', // Mavi-mor (SlateBlue)
                '#9370DB', // Orta mor (MediumPurple)
                '#A9A9A9'  // Koyu gri (Diğerleri için)
            ];

            if (salesByChannelChart) {
                salesByChannelChart.destroy();
            }
            salesByChannelChart = new Chart(salesByChannelCtx, {
                type: 'doughnut',
                data: {
                    labels: data.labels,
                    datasets: [{
                        data: data.values,
                        backgroundColor: channelColors.slice(0, data.labels.length), // Veri sayısına göre renkleri kullan
                        borderColor: CHART_COLORS.cardBg, // Kart arka planıyla aynı renk
                        borderWidth: 5, // Dilimler arası boşluk
                        hoverOffset: 10 // Hover efekti
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    cutout: '70%',
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            backgroundColor: CHART_COLORS.cardBg,
                            titleColor: CHART_COLORS.textLight,
                            bodyColor: CHART_COLORS.textLight,
                            borderColor: CHART_COLORS.secondaryPurple,
                            borderWidth: 1,
                            cornerRadius: 6,
                            padding: 10,
                            callbacks: {
                                label: function(context) {
                                    let label = context.label || '';
                                    if (label) {
                                        label += ': ';
                                    }
                                    if (context.parsed !== null) {
                                        label += context.parsed + '%';
                                    }
                                    return label;
                                }
                            }
                        }
                    }
                }
            });
            createChannelLegends(data.labels, channelColors.slice(0, data.labels.length));
        }
    }

    /**
     * Kanallara Göre Satış grafiği için özel legend oluşturma
     * @param {Array<string>} labels - Kanal etiketleri
     * @param {Array<string>} colors - Kanal renkleri
     */
    function createChannelLegends(labels, colors) {
        const legendContainer = document.getElementById('channelLegends');
        legendContainer.innerHTML = '';
        labels.forEach((label, index) => {
            const legendItem = document.createElement('div');
            legendItem.className = 'd-flex align-items-center me-3 my-1'; // Küçük boşluklar için my-1
            legendItem.innerHTML = `
                <span style="display:inline-block; width:12px; height:12px; background-color:${colors[index]}; border-radius:3px; margin-right:8px;"></span>
                <span class="text-white-50 small">${label}</span>
            `;
            legendContainer.appendChild(legendItem);
        });
    }

    /**
     * En Çok Satan Ürünleri doldurma fonksiyonu
     * @param {Array<Object>} products - Ürün verileri
     */
    function updateTopSellingProducts(products) {
        const productsContainer = document.getElementById('topSellingProducts');
        productsContainer.innerHTML = '';

        if (products && products.length > 0) {
            products.forEach(product => {
                const colDiv = document.createElement('div');
                colDiv.className = 'col-md-2 col-4 mb-3'; // Responsive olarak küçük ekranlarda 2'li düzen
                colDiv.innerHTML = `
                    <div class="product-item">
                        <div class="product-icon mb-2">
                            <img src="${product.iconUrl || 'https://via.placeholder.com/60/ccc/fff?text=' + product.name.charAt(0)}" alt="${product.name}" class="img-fluid rounded-circle product-icon-img" style="background-color:${product.iconBgColor || '#3e2e60'};">
                        </div>
                        <h6 class="text-light product-name">${product.name}</h6>
                        <p class="text-muted-subtle small product-price">₺${product.price.toLocaleString('tr-TR')}</p>
                    </div>
                `;
                productsContainer.appendChild(colDiv);
            });
        } else {
            productsContainer.innerHTML = '<div class="col-12 text-center text-muted">Ürün bulunamadı.</div>';
        }
    }

    // Tüm verileri API'den çekip paneli güncelleyen ana fonksiyon
    async function loadDashboardData() {
        const metricsData = await fetchData(API_ENDPOINTS.metrics);
        updateMetrics(metricsData);

        const revenueTrendData = await fetchData(API_ENDPOINTS.revenueTrend);
        updateRevenueTrendChart(revenueTrendData);

        const salesByChannelData = await fetchData(API_ENDPOINTS.salesByChannel);
        // Kanal renklerini JS'de tanımladığımız channelColors dizisinden alacağız
        if (salesByChannelData) {
            // salesByChannelData.colors'ı API'den almasak bile JS'de tanımladığımızı kullanırız
            updateSalesByChannelChart(salesByChannelData);
        }

        const topSellingProductsData = await fetchData(API_ENDPOINTS.topSellingProducts);
        updateTopSellingProducts(topSellingProductsData);
    }

    // Sayfa yüklendiğinde verileri çek
    loadDashboardData();

    // Opsiyonel: Belirli aralıklarla verileri yenilemek için
    // setInterval(loadDashboardData, 60000); // Her 1 dakikada bir yenile
});

// *******************************************************************
// Gerçek API'niz hazır olana kadar kullanabileceğiniz Mock Veriler
// Eğer gerçek API kullanacaksanız, fetchData fonksiyonundaki yorum satırlarını kaldırın
// ve aşağıdaki MOCK_API_RESPONSES objesini ve ilgili mock fetch satırını silin.
// *******************************************************************
const MOCK_API_RESPONSES = {
    '/dashboard/metrics': {
        totalRevenue: '₺2.4M',
        conversionRate: '3.47%',
        averageBasket: '₺347',
        totalOrders: '6,847',
        totalRevenueChange: 12.5,
        conversionRateChange: 3.2,
        averageBasketChange: 8.1,
        totalOrdersChange: 15.3
    },
    '/dashboard/revenue-trend': {
        labels: ['Oca', 'Şub', 'Mar', 'Nis', 'May', 'Haz'],
        values: [350000, 380000, 360000, 420000, 450000, 480000]
    },
    '/dashboard/sales-by-channel': {
        labels: ['Web Sitesi', 'Mobil App', 'Sosyal Medya', 'Mağaza'],
        values: [40, 25, 20, 15] // Yüzde olarak toplam 100 olmalı
    },
    '/dashboard/top-selling-products': [
        {
            name: 'Laptop Pro',
            price: 45600,
            iconUrl: 'https://via.placeholder.com/60/7B68EE/FFFFFF?text=L', // Görseldeki renkleri yakala
            iconBgColor: '#7B68EE' // MediumSlateBlue
        },
        {
            name: 'Akıllı Telefon',
            price: 28900,
            iconUrl: 'https://via.placeholder.com/60/66CDAA/FFFFFF?text=T', // MediumAquaMarine
            iconBgColor: '#66CDAA'
        },
        {
            name: 'Kulaklık',
            price: 1250,
            iconUrl: 'https://via.placeholder.com/60/9370DB/FFFFFF?text=H', // MediumPurple
            iconBgColor: '#9370DB'
        },
        {
            name: 'Tablet',
            price: 8750,
            iconUrl: 'https://via.placeholder.com/60/ADD8E6/FFFFFF?text=B', // LightBlue
            iconBgColor: '#ADD8E6'
        },
        {
            name: 'Kamera',
            price: 15600,
            iconUrl: 'https://via.placeholder.com/60/FF6347/FFFFFF?text=C', // Tomato
            iconBgColor: '#FF6347'
        },
        {
            name: 'Akıllı Saat',
            price: 7200,
            iconUrl: 'https://via.placeholder.com/60/FFD700/FFFFFF?text=W', // Gold
            iconBgColor: '#FFD700'
        }
    ]
};