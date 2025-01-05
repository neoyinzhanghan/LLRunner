import os
from tensorboard.backend.event_processing import event_accumulator
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib import patheffects
from collections import defaultdict

# Set up premium plotting style
plt.style.use('seaborn-white')
mpl.rcParams.update({
    'font.family': 'Helvetica Neue',
    'font.weight': 'light',
    'font.size': 11,
    'axes.linewidth': 0.8,
    'axes.labelweight': 'light',
    'axes.grid': True,
    'grid.alpha': 0.2,
    'grid.linestyle': '-',
    'xtick.major.width': 0.8,
    'ytick.major.width': 0.8,
    'xtick.minor.width': 0.5,
    'ytick.minor.width': 0.5,
    'xtick.major.size': 3.0,
    'ytick.major.size': 3.0,
    'xtick.minor.size': 2.0,
    'ytick.minor.size': 2.0,
    'legend.frameon': False,
    'legend.fontsize': 9,
    'axes.spines.top': False,
    'axes.spines.right': False
})

def create_premium_plot(epoch_avg):
    """Create a premium-looking plot with two subplots side by side."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), dpi=300)
    fig.patch.set_facecolor('#FFFFFF')
    
    # Premium color scheme
    train_color = '#006D77'  # Deep teal
    val_color = '#E29578'    # Coral
    epoch_line_color = '#800000'  # Maroon
    
    # Function to style each subplot
    def style_subplot(ax, ylim=None, title=""):
        ax.set_facecolor('#FFFFFF')
        
        # Plot with premium styling
        train_line = ax.plot(epoch_avg['epoch'], epoch_avg['train_loss'],
                label='Training Loss',
                color=train_color,
                linewidth=1.8,
                alpha=0.95)
        
        train_line[0].set_path_effects([patheffects.SimpleLineShadow(alpha=0.2, offset=(0, -0.5)),
                                       patheffects.Normal()])
        
        val_line = ax.plot(epoch_avg['epoch'], epoch_avg['val_loss'],
                label='Validation Loss',
                color=val_color,
                linewidth=1.8,
                alpha=0.95)
        
        val_line[0].set_path_effects([patheffects.SimpleLineShadow(alpha=0.2, offset=(0, -0.5)),
                                     patheffects.Normal()])
        
        # Add vertical line at epoch 50 with pastel red color
        ax.axvline(x=50, color=epoch_line_color, linestyle='--', linewidth=1.5, alpha=0.8)
        # Add subtle text annotation for the vertical line
        ax.text(50.5, ax.get_ylim()[1], 'Epoch 50', 
                rotation=90, 
                verticalalignment='top',
                color=epoch_line_color,
                alpha=0.8,
                fontsize=9)
        
        # Customized grid
        ax.grid(True, linestyle='-', alpha=0.15, color='#2F3132', linewidth=0.5)
        
        # Spine styling
        for spine in ax.spines.values():
            spine.set_color('#2F3132')
            spine.set_linewidth(0.8)
        
        # Title and labels with premium typography
        ax.set_title(title, 
                    fontsize=13, 
                    pad=5, 
                    color='#2F3132', 
                    fontweight='light',
                    loc='left')
        
        ax.set_xlabel('Epoch', fontsize=11, labelpad=10, color='#2F3132', fontweight='light')
        ax.set_ylabel('Loss', fontsize=11, labelpad=10, color='#2F3132', fontweight='light')
        
        # Custom tick parameters
        ax.tick_params(axis='both', which='major', labelsize=9, colors='#2F3132', length=4)
        ax.tick_params(axis='both', which='minor', labelsize=8, colors='#2F3132', length=2)
        ax.minorticks_on()
        
        if ylim is not None:
            ax.set_ylim(ylim)
        
        # Elegant legend
        leg = ax.legend(loc='upper right',
                       fontsize=9,
                       framealpha=0)
        
        for text in leg.get_texts():
            text.set_color('#2F3132')
            text.set_fontweight('light')
    
    # Style both subplots
    style_subplot(ax1, ylim=None, title="Model Training Progress (Full Range)")
    style_subplot(ax2, ylim=(0, 0.8), title="Model Training Progress (Detailed View)")
    
    # Adjust layout and spacing
    plt.subplots_adjust(top=0.98, bottom=0.08, hspace=0.2, left=0.1, right=0.95)
    
    # Save with minimal padding
    current_dir = os.getcwd()
    plt.savefig(os.path.join(current_dir, 'loss_plot_premium_combined.png'), 
                dpi=300,
                bbox_inches=None,
                pad_inches=0,
                facecolor='white',
                edgecolor='none')
    plt.savefig(os.path.join(current_dir, 'loss_plot_premium_combined.pdf'), 
                format='pdf',
                bbox_inches=None,
                pad_inches=0,
                facecolor='white',
                edgecolor='none')
    
    plt.show()

def extract_tensorboard_metrics(event_files, output_csv="training_metrics.csv"):
    """Extract and process training metrics from Tensorboard files."""
    train_data = []
    val_data = []
    steps_to_epoch = {}
    
    for event_file in event_files:
        if not os.path.exists(event_file):
            print(f"Warning: File {event_file} does not exist, skipping...")
            continue
            
        try:
            print(f"Processing file: {event_file}")
            ea = event_accumulator.EventAccumulator(event_file)
            ea.Reload()
            
            tags = ea.Tags()
            print(f"Available tags: {tags['scalars']}")
            
            if 'epoch' in tags['scalars']:
                epoch_events = ea.Scalars('epoch')
                for event in epoch_events:
                    steps_to_epoch[event.step] = event.value
            
            if 'train_loss' in tags['scalars']:
                train_events = ea.Scalars('train_loss')
                for event in train_events:
                    train_data.append({
                        'step': event.step,
                        'epoch': steps_to_epoch.get(event.step, event.step),
                        'train_loss': event.value
                    })
                    
            if 'val_loss' in tags['scalars']:
                val_events = ea.Scalars('val_loss')
                for event in val_events:
                    val_data.append({
                        'step': event.step,
                        'epoch': steps_to_epoch.get(event.step, event.step),
                        'val_loss': event.value
                    })
                    
        except Exception as e:
            print(f"Error processing file {event_file}: {str(e)}")
            continue
    
    train_df = pd.DataFrame(train_data) if train_data else pd.DataFrame(columns=['step', 'epoch', 'train_loss'])
    val_df = pd.DataFrame(val_data) if val_data else pd.DataFrame(columns=['step', 'epoch', 'val_loss'])
    
    df = pd.merge(train_df, val_df, on=['step', 'epoch'], how='outer')
    df = df.sort_values('step').reset_index(drop=True)
    
    # Calculate epoch-wise averages
    epoch_avg = df.groupby('epoch').agg({
        'train_loss': 'mean',
        'val_loss': 'mean'
    }).reset_index()
    
    # Create combined plot with premium styling
    create_premium_plot(epoch_avg)
    
    # Save data files in current directory
    current_dir = os.getcwd()
    df.to_csv(os.path.join(current_dir, output_csv), index=False)
    print(f"\nRaw metrics saved to {output_csv}")
    
    avg_csv = output_csv.replace('.csv', '_epoch_averages.csv')
    epoch_avg.to_csv(os.path.join(current_dir, avg_csv), index=False)
    print(f"Epoch-wise averages saved to {avg_csv}")
    
    return df, epoch_avg

# Your event files
event_files = [
    "/Users/neo/Downloads/2024-06-11  DeepHemeRetrain non-frog feature deploy 2/1/version_0/events.out.tfevents.1717748286.path-lambda1.2270305.0",
    "/Users/neo/Downloads/2024-06-11  DeepHemeRetrain non-frog feature deploy 2/1/version_0/events.out.tfevents.1717756274.path-lambda1.2270305.1",
]

# Extract metrics and create premium plots
df, epoch_avg = extract_tensorboard_metrics(event_files)
