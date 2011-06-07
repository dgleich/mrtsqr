%% 
% Test the following regression problem on tiny images, in prep for a big 
% MapReduce job for the entire collection:
%
% We are going to regress the sum of red values of an image an the
% gray values of an image.
% i.e.
%   let red(i) = sum over pixels red(pixel) be defined for each image
%   then we want to find coefficients such that
%   red(i) = sum over pixels gray(pixel)*coeff(pixel)
% this is a very tall and skinny least squares problem.

%%
% Setup tiny images
tinyimages_path = '~/data/sandia/tinyimages';
addpath(tinyimages_path)

%%
% Setup colormap
cmap = [
    0.0431    0.5176    0.7804
    0.0443    0.5064    0.7729
    0.0454    0.4951    0.7655
    0.0466    0.4839    0.7580
    0.0477    0.4726    0.7505
    0.0488    0.4614    0.7431
    0.0500    0.4501    0.7356
    0.0511    0.4388    0.7281
    0.0522    0.4276    0.7207
    0.0534    0.4163    0.7132
    0.0545    0.4051    0.7058
    0.0557    0.3938    0.6983
    0.0568    0.3825    0.6908
    0.0579    0.3713    0.6834
    0.0591    0.3600    0.6759
    0.0602    0.3488    0.6684
    0.0614    0.3375    0.6610
    0.0625    0.3262    0.6535
    0.0636    0.3150    0.6460
    0.0648    0.3037    0.6386
    0.0659    0.2925    0.6311
    0.0670    0.2812    0.6237
    0.0682    0.2700    0.6162
    0.0693    0.2587    0.6087
    0.0705    0.2474    0.6013
    0.0716    0.2362    0.5938
    0.0727    0.2249    0.5863
    0.0739    0.2137    0.5789
    0.0750    0.2024    0.5714
    0.0762    0.1911    0.5639
    0.0773    0.1799    0.5565
    0.0784    0.1686    0.5490
    0.0947    0.1696    0.5319
    0.1110    0.1706    0.5147
    0.1273    0.1716    0.4975
    0.1436    0.1725    0.4804
    0.1599    0.1735    0.4632
    0.1762    0.1745    0.4461
    0.1925    0.1755    0.4289
    0.2088    0.1765    0.4118
    0.2251    0.1775    0.3946
    0.2414    0.1784    0.3775
    0.2577    0.1794    0.3603
    0.2740    0.1804    0.3431
    0.2903    0.1814    0.3260
    0.3066    0.1824    0.3088
    0.3229    0.1833    0.2917
    0.3392    0.1843    0.2745
    0.3555    0.1853    0.2574
    0.3718    0.1863    0.2402
    0.3881    0.1873    0.2230
    0.4044    0.1882    0.2059
    0.4207    0.1892    0.1887
    0.4370    0.1902    0.1716
    0.4533    0.1912    0.1544
    0.4696    0.1922    0.1373
    0.4859    0.1931    0.1201
    0.5022    0.1941    0.1029
    0.5185    0.1951    0.0858
    0.5348    0.1961    0.0686
    0.5511    0.1971    0.0515
    0.5674    0.1980    0.0343
    0.5837    0.1990    0.0172
    0.6000    0.2000         0];


%% 
% Load images
imgs = loadTinyImages(1:40001, fullfile(tinyimages_path,'tiny_images.bin'));
imgs = double(imgs);

%%
% Compute gray values
G = zeros(size(imgs,4),size(imgs,1)*size(imgs,2));
r = zeros(size(imgs,4),1);

for i=1:size(imgs,4)
    redval = sum(sum(imgs(:,:,1,i)));
    grayvals = imgs(:,:,1,i)*0.299 + imgs(:,:,2,i)*0.587 + imgs(:,:,3,i)*0.114;
    G(i,:) = double(grayvals(:))/255;
    r(i) = redval;
end
    
%%
% Solve the regression
y = G\r;

%%
% Plot the solution
imagesc(reshape(y,32,32)); colormap(cmap); colorbar