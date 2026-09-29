import React, { useState, useEffect } from 'react';
import { Car, Map, Clock, DollarSign, Activity, Zap, Navigation } from 'lucide-react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import './index.css';

// Mock Data for the Kafka Streaming Heatmap
const generateMockStreamData = () => {
  const data = [];
  let currentDemand = 500;
  for (let i = 24; i >= 0; i--) {
    const time = new Date(Date.now() - i * 5 * 60000); // 5 min windows
    currentDemand = currentDemand + (Math.random() * 100 - 50);
    if(currentDemand < 100) currentDemand = 100;
    data.push({
      time: time.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      demand: Math.floor(currentDemand),
      predicted_demand: Math.floor(currentDemand * (1 + (Math.random() * 0.1 - 0.05)))
    });
  }
  return data;
};

const PredictorForm = () => {
  const [loading, setLoading] = useState(false);
  const [prediction, setPrediction] = useState(null);

  const handlePredict = (e) => {
    e.preventDefault();
    setLoading(true);
    setPrediction(null);
    
    // Simulate API call to XGBoost models via Spark/HDFS
    setTimeout(() => {
      setPrediction({
        fare: (Math.random() * 30 + 15).toFixed(2),
        eta: (Math.random() * 15 + 10).toFixed(0)
      });
      setLoading(false);
    }, 1500);
  };

  return (
    <div className="glass-panel">
      <div className="panel-title">
        <Zap className="text-accent-cyan" size={24} color="#22d3ee" />
        XGBoost Route Predictor
      </div>
      <p style={{ color: 'var(--text-secondary)', marginBottom: '1.5rem', fontSize: '0.9rem' }}>
        Target Encoded ML model trained on 150M+ trips
      </p>
      
      <form onSubmit={handlePredict}>
        <div className="form-group">
          <label><Map size={14} style={{ display:'inline', marginRight:'4px' }}/> Pickup Zone (Location ID)</label>
          <input type="number" className="form-input" placeholder="e.g. 236 (Upper East Side)" required defaultValue="236"/>
        </div>
        
        <div className="form-group">
          <label><Navigation size={14} style={{ display:'inline', marginRight:'4px' }}/> Dropoff Zone (Location ID)</label>
          <input type="number" className="form-input" placeholder="e.g. 132 (JFK Airport)" required defaultValue="132"/>
        </div>

        <div className="form-group">
          <label><Clock size={14} style={{ display:'inline', marginRight:'4px' }}/> Departure Time</label>
          <input type="time" className="form-input" required defaultValue="17:30"/>
        </div>

        <button type="submit" className="primary-btn" disabled={loading}>
          {loading ? 'Crunching XGBoost Trees...' : 'Predict Fare & ETA'}
        </button>
      </form>

      {prediction && (
        <div className="prediction-result">
          <div className="metric-card">
            <h3>Predicted Fare</h3>
            <div className="value" style={{color: '#c084fc'}}>${prediction.fare}</div>
            <p style={{fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '4px'}}>± $1.67 MAE</p>
          </div>
          <div className="metric-card">
            <h3>Estimated ETA</h3>
            <div className="value">{prediction.eta} min</div>
            <p style={{fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '4px'}}>± 2.83 min MAE</p>
          </div>
        </div>
      )}
    </div>
  );
};

const DemandHeatmap = () => {
  const [data, setData] = useState([]);

  useEffect(() => {
    setData(generateMockStreamData());
    
    // Simulate real-time streaming updates every 5 seconds
    const interval = setInterval(() => {
      setData(prev => {
        const newData = [...prev.slice(1)];
        const lastDemand = prev[prev.length - 1].demand;
        const newDemand = Math.max(100, lastDemand + (Math.random() * 100 - 50));
        newData.push({
          time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
          demand: Math.floor(newDemand),
          predicted_demand: Math.floor(newDemand * (1 + (Math.random() * 0.1 - 0.05)))
        });
        return newData;
      });
    }, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="glass-panel" style={{ height: '100%' }}>
      <div className="panel-title">
        <Activity className="text-accent-purple" size={24} color="#c084fc" />
        Live Manhattan Demand Heatmap
      </div>
      <p style={{ color: 'var(--text-secondary)', marginBottom: '1rem', fontSize: '0.9rem' }}>
        Spark Structured Streaming | 5-Minute Sliding Windows via Apache Kafka
      </p>
      
      <div className="chart-container">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="colorDemand" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#22d3ee" stopOpacity={0.8}/>
                <stop offset="95%" stopColor="#22d3ee" stopOpacity={0}/>
              </linearGradient>
              <linearGradient id="colorPredicted" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#c084fc" stopOpacity={0.8}/>
                <stop offset="95%" stopColor="#c084fc" stopOpacity={0}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" />
            <XAxis dataKey="time" stroke="#94a3b8" fontSize={12} />
            <YAxis stroke="#94a3b8" fontSize={12} />
            <Tooltip 
              contentStyle={{ backgroundColor: 'rgba(15, 23, 42, 0.9)', border: '1px solid rgba(148, 163, 184, 0.2)', borderRadius: '8px' }}
              itemStyle={{ color: '#f8fafc' }}
            />
            <Area type="monotone" dataKey="predicted_demand" name="Predicted Demand" stroke="#c084fc" fillOpacity={1} fill="url(#colorPredicted)" />
            <Area type="monotone" dataKey="demand" name="Actual Live Trips" stroke="#22d3ee" fillOpacity={1} fill="url(#colorDemand)" />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

function App() {
  return (
    <div className="dashboard-container">
      <header>
        <div className="logo-section">
          <h1><Car color="#22d3ee" size={36} /> TaxiPulse</h1>
          <p>Real-Time NYC Big Data Analytics & Prediction Engine</p>
        </div>
        <div style={{display: 'flex', gap: '1rem'}}>
          <div style={{background: 'rgba(34, 211, 238, 0.1)', color: '#22d3ee', padding: '0.5rem 1rem', borderRadius: '20px', fontSize: '0.85rem', fontWeight: 'bold', display: 'flex', alignItems: 'center', gap: '0.5rem'}}>
            <span style={{width: '8px', height: '8px', borderRadius: '50%', background: '#22d3ee', display: 'inline-block', boxShadow: '0 0 8px #22d3ee'}}></span>
            Kafka Connected
          </div>
          <div style={{background: 'rgba(192, 132, 252, 0.1)', color: '#c084fc', padding: '0.5rem 1rem', borderRadius: '20px', fontSize: '0.85rem', fontWeight: 'bold', display: 'flex', alignItems: 'center', gap: '0.5rem'}}>
            <span style={{width: '8px', height: '8px', borderRadius: '50%', background: '#c084fc', display: 'inline-block', boxShadow: '0 0 8px #c084fc'}}></span>
            Spark YARN Active
          </div>
        </div>
      </header>

      <div className="sidebar">
        <PredictorForm />
      </div>

      <div className="main-content">
        <DemandHeatmap />
      </div>
    </div>
  );
}

export default App;
